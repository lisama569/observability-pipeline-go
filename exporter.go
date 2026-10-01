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
	// DropRetryExhausted 暂时性失败重试到预算用尽仍无法送达。
	DropRetryExhausted
	// DropCanceled 管线关闭或上下文取消导致丢弃。
	DropCanceled
	// DropPermanentlyRejected 接收端明确拒绝这批数据（永久失败），
	// 立即停止重试，与“重试耗尽”明确区分。
	DropPermanentlyRejected
)

func (r DropReason) String() string {
	switch r {
	case DropQueueFull:
		return "queue_full"
	case DropRetryExhausted:
		return "retry_exhausted"
	case DropCanceled:
		return "canceled"
	default:
		return "permanently_rejected"
	}
}

// DropHandler 在数据被丢弃时回调，携带可区分的原因。
type DropHandler func(b Batch, reason DropReason)

// Dispatcher 提供有界队列、背压、失败分级重试与投递去重。
// 队列容量固定：导出慢时采集侧在队列满后直接丢弃并回调原因，不无界堆积。
type Dispatcher struct {
	exp     Exporter
	onDrop  DropHandler
	retries int
	backoff time.Duration
	dedup   *Deduper

	queue  chan Batch
	done   chan struct{}
	closed sync.Once
	wg     sync.WaitGroup
	// inflight 记录已接收但未完成的批次数（队列中 + 导出中）。
	inflight atomic.Int64
}

func NewDispatcher(exp Exporter, queueSize, retries, dedupCap int, onDrop DropHandler) *Dispatcher {
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
		dedup:   NewDeduper(dedupCap),
		queue:   make(chan Batch, queueSize),
		done:    make(chan struct{}),
	}
}

// Submit 非阻塞投递；队列满时按策略丢弃并回调 DropQueueFull。
// 已有终态结论（已送达/已拒收）的批次在入队前即被静默抑制：不占队列
// 容量、不报任何丢弃——重投一个被明确拒收过的批次，绝不可能再产生
// 一次 queue_full；在途重复不做此抑制，主导者失败时要靠等待者接管。
func (d *Dispatcher) Submit(b Batch) {
	select {
	case <-d.done:
		d.onDrop(b, DropCanceled)
		return
	default:
	}
	// 入队前非阻塞终态抑制：已送达/已拒收批次的重投不进队列、不报丢弃。
	if _, ok := d.dedup.KnownTerminal(b.ID); ok {
		return
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
	// 投递前去重：已送达过、被永久拒收过的批次与并发中的重复批次都不再
	// 调用接收端，由管线自身（而非调用方）保证同一 Batch.ID 最终只可见
	// 一次；重复的永久拒收也不会再上报一次丢弃。
	leader, _, finish := d.dedup.BeginEx(b.ID)
	if !leader {
		return
	}
	outcome := OutcomeUndelivered
	defer func() { finish(outcome) }()
	attempts := d.retries + 1
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			d.onDrop(b, DropCanceled)
			return
		}
		err := d.exp.Export(ctx, b)
		switch {
		case err == nil:
			outcome = OutcomeDelivered
			return
		case ctx.Err() != nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// 生命周期终止优先于永久失败判定，不按业务拒绝处理；
			// 不入去重表，之后重新投递仍会正常送到接收端。
			d.onDrop(b, DropCanceled)
			return
		case IsPermanent(err):
			// 接收端明确不要这批数据：立即停止，不再消耗重试预算与退避时间；
			// ID 记入去重表，后续同一批次的重复投递不再打到接收端，
			// 也不会再产生一次丢弃上报。
			outcome = OutcomeRejected
			d.onDrop(b, DropPermanentlyRejected)
			return
		}
		// 暂时性失败：指数退避后重试，预算用尽才丢弃。
		select {
		case <-ctx.Done():
			d.onDrop(b, DropCanceled)
			return
		case <-time.After(d.backoff * time.Duration(1<<i)):
		}
	}
	d.onDrop(b, DropRetryExhausted)
}

// Close 停止消费并等待在途导出完成。
func (d *Dispatcher) Close() {
	d.closed.Do(func() { close(d.done) })
	d.wg.Wait()
}
