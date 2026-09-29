package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Batch 是一组待导出的记录。
type Batch struct {
	ID      uint64
	Version uint16
	Source  string // v2 新增字段
	Records []Record
	// Unknown 保留解码时遇到的未知字段，供版本演进使用。
	Unknown map[string][]byte
}

// Exporter 是导出端接口。
type Exporter interface {
	Export(ctx context.Context, b Batch) error
}

// DropReason 区分数据被丢弃的原因。
type DropReason int

const (
	// DropQueueFull 队列满，背压触发丢弃。
	DropQueueFull DropReason = iota
	// DropRetryExhausted 暂时性失败重试预算耗尽，无法送达。
	DropRetryExhausted
	// DropCanceled 管线关闭或上下文取消导致丢弃。
	DropCanceled
	// DropPermanent 接收端明确拒绝该批次（永久失败），不重试直接丢弃。
	DropPermanent
)

func (r DropReason) String() string {
	switch r {
	case DropQueueFull:
		return "queue_full"
	case DropRetryExhausted:
		return "retry_exhausted"
	case DropCanceled:
		return "canceled"
	case DropPermanent:
		return "permanent_rejected"
	default:
		return "unknown"
	}
}

// DropHandler 在数据被丢弃时回调，携带可区分的原因。
type DropHandler func(b Batch, reason DropReason)

// Dispatcher 提供有界队列、背压、失败分级重试与批次级去重。
// 队列容量固定：导出慢时采集侧在队列满后直接丢弃并回调原因，不无界堆积。
type Dispatcher struct {
	exp     Exporter
	onDrop  DropHandler
	retries int
	backoff time.Duration
	dedup   *dedupTable

	queue  chan Batch
	done   chan struct{}
	closed sync.Once
	wg     sync.WaitGroup
	// inflight 记录已接收但未完成的批次数（队列中 + 导出中）。
	inflight atomic.Int64
}

func NewDispatcher(exp Exporter, queueSize, retries int, onDrop DropHandler) *Dispatcher {
	if queueSize < 1 {
		queueSize = 1
	}
	if onDrop == nil {
		onDrop = func(Batch, DropReason) {}
	}
	return &Dispatcher{
		exp:     exp,
		onDrop:  onDrop,
		retries: retries,
		backoff: 5 * time.Millisecond,
		dedup:   newDedupTable(DefaultDedupCap),
		queue:   make(chan Batch, queueSize),
		done:    make(chan struct{}),
	}
}

// setDedupCap 在 Start 前调整去重状态容量；cap<=0 取默认值。
func (d *Dispatcher) setDedupCap(cap int) { d.dedup = newDedupTable(cap) }

// Submit 非阻塞投递；队列满时按策略丢弃并回调 DropQueueFull。
func (d *Dispatcher) Submit(b Batch) {
	select {
	case <-d.done:
		d.onDrop(b, DropCanceled)
		return
	default:
	}
	select {
	case d.queue <- b:
		d.inflight.Add(1)
	case <-d.done:
		d.onDrop(b, DropCanceled)
	default:
		d.onDrop(b, DropQueueFull)
	}
}

// Inflight 返回已接收但未完成的批次数。
func (d *Dispatcher) Inflight() int64 { return d.inflight.Load() }

// Start 启动消费循环；必须在 Close 之前调用，保证 WaitGroup 计数先行。
func (d *Dispatcher) Start(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.run(ctx)
	}()
}

// run 消费队列并带重试导出，直到 Close 且队列排空。
// 重试针对同一 Batch.ID 进行：管线自身在去重状态表上保证同一批次最终在
// 接收端只可见一次（singleflight 合并并发导出 + 终结 ID 抑制重复投递），
// 不再依赖调用方自行按 ID 去重。
func (d *Dispatcher) run(ctx context.Context) {
	for {
		select {
		case b := <-d.queue:
			d.exportWithRetry(ctx, b)
		case <-d.done:
			// 排空剩余队列后退出
			for {
				select {
				case b := <-d.queue:
					d.exportWithRetry(ctx, b)
				default:
					return
				}
			}
		}
	}
}

func (d *Dispatcher) exportWithRetry(ctx context.Context, b Batch) {
	defer d.inflight.Add(-1)
	for {
		a, outcome := d.dedup.begin(b.ID)
		switch outcome {
		case outcomeDup:
			// 该批次此前已送达或已被永久拒绝：幂等重复，不触达接收端，
			// 也不再次上报丢弃（终结结论此前已上报过一次）。
			return
		case outcomeFollower:
			finalized, err := a.wait(ctx)
			if err != nil {
				// 本调用方的等待先于结论被取消（不影响 leader 的导出）。
				d.onDrop(b, DropCanceled)
				return
			}
			if finalized {
				// 成功（送达）或永久拒绝（leader 已上报过原因），跟随结论即可。
				return
			}
			// leader 暂时性放弃；重新登记，由本调用方接管重试，避免丢批。
			continue
		}

		// leader：由本调用方负责整轮重试。
		err := d.exportAttempts(ctx, b)
		switch {
		case err == nil:
			d.dedup.finalize(b.ID, a, nil)
			return
		case IsPermanentFailure(err):
			// 接收端明确拒绝：立即停止，永久失败标记长期去重。
			d.dedup.finalize(b.ID, a, err)
			d.onDrop(b, DropPermanent)
			return
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// 取消/超时：不保留去重标记，以取消原因丢弃。
			d.dedup.abandon(b.ID, a, err)
			d.onDrop(b, DropCanceled)
			return
		default:
			// 暂时性失败且重试预算耗尽：不保留去重标记，后续重新投递仍可再试。
			d.dedup.abandon(b.ID, a, err)
			d.onDrop(b, DropRetryExhausted)
			return
		}
	}
}

// exportAttempts 执行一次实际导出尝试 + 指数退避重试，仅暂时性失败会重试。
// 返回 nil（成功）、永久失败错误（包装或实现 PermanentFailure）或暂时性错误。
// 重试期间 ctx 取消时返回该取消错误。
func (d *Dispatcher) exportAttempts(ctx context.Context, b Batch) error {
	attempts := d.retries + 1
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := d.exp.Export(ctx, b)
		if err == nil {
			return nil
		}
		lastErr = err
		switch {
		case IsPermanentFailure(err):
			return err
		case ctx.Err() != nil:
			// 导出过程中上下文取消/超时，不再退避。
			return ctx.Err()
		case i == attempts-1:
			return lastErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.backoff * time.Duration(1<<i)):
		}
	}
	return lastErr
}

// Close 停止消费并等待在途导出完成。
func (d *Dispatcher) Close() {
	d.closed.Do(func() { close(d.done) })
	d.wg.Wait()
}
