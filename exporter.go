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

// DispatcherOption 配置 Dispatcher 的可选能力，新增能力通过选项提供，
// 不改变 NewDispatcher 的位置参数，既有调用方无需改动。
type DispatcherOption func(*dispatcherConfig)

type dispatcherConfig struct {
	dedupCap int
}

// WithDedupCap 配置投递去重表容量（LRU 淘汰），<=0 时使用 DefaultDedupCap。
// 去重表始终存在：成功送达与被明确拒收的批次 ID 都会被记住，
// 该选项只控制容量上限。
func WithDedupCap(capacity int) DispatcherOption {
	return func(c *dispatcherConfig) { c.dedupCap = capacity }
}

// NewDispatcher 创建导出派发器。
//
// 位置参数保持历史签名 (exp, queueSize, retries, onDrop)，老代码不改也能
// 编译运行；新增能力以可选项提供，例如：
//
//	NewDispatcher(exp, 64, 3, onDrop, WithDedupCap(1024))
//
// 不显式给选项时，去重表以 DefaultDedupCap 容量启用（成功去重与拒收抑制
// 都生效）。
func NewDispatcher(exp Exporter, queueSize, retries int, onDrop DropHandler, opts ...DispatcherOption) *Dispatcher {
	if queueSize < 1 {
		queueSize = 1
	}
	if onDrop == nil {
		onDrop = func(Batch, DropReason) {}
	}
	cfg := dispatcherConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Dispatcher{
		exp:     exp,
		onDrop:  onDrop,
		retries: retries,
		backoff: 5 * time.Millisecond,
		dedup:   NewDeduper(cfg.dedupCap),
		queue:   make(chan Batch, queueSize),
		done:    make(chan struct{}),
	}
}

// Submit 非阻塞投递；队列满时按策略丢弃并回调 DropQueueFull。
// 已成功送达或已被接收端明确拒收的批次在入队前即被静默抑制：
// 不再打到接收端、不占用队列，也不重复上报丢弃。
func (d *Dispatcher) Submit(b Batch) {
	if d.dedup.IsSettled(b.ID) {
		return
	}
	select {
	case <-d.done:
		d.onDrop(b, DropCanceled)
		return
	default:
	}
	select {
	case d.queue <- b:
		d.inflight.Add(1)
		// 入队与消费之间若主导者恰好把终态写入，消费侧 Begin 会兜底抑制，
		// 接收端最多可见一次。
	case <-d.done:
		d.onDrop(b, DropCanceled)
	default:
		if !d.dedup.IsSettled(b.ID) {
			d.onDrop(b, DropQueueFull)
		}
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
	// 投递前去重：已送达/已拒收过的批次与并发中的重复批次都不再调用接收端，
	// 由管线自身（而非调用方）保证同一 Batch.ID 最终只可见一次。
	// 被拒收批次的重复投递在此静默跳过，丢弃回调只在首次拒收时发生一次。
	leader, finish := d.dedup.Begin(b.ID)
	if !leader {
		return
	}
	outcome := OutcomeUnsettled
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
			// 属于未决结论，后续重新投递仍会正常送出。
			d.onDrop(b, DropCanceled)
			return
		case IsPermanent(err):
			// 接收端明确不要这批数据：立即停止，不再消耗重试预算与退避时间；
			// ID 记入去重表，后续重复投递静默抑制，丢弃只上报这一次。
			d.onDrop(b, DropPermanentlyRejected)
			outcome = OutcomeRejected
			return
		}
		// 暂时性失败：指数退避后重试，预算用尽才丢弃（结论仍为未决，
		// 之后重新投递会被当作新批次正常送出）。
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
