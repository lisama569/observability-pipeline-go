package pipeline

import (
	"context"
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
	// DropRetryExhausted 重试耗尽，无法送达。
	DropRetryExhausted
	// DropCanceled 管线关闭或上下文取消导致丢弃。
	DropCanceled
)

func (r DropReason) String() string {
	switch r {
	case DropQueueFull:
		return "queue_full"
	case DropRetryExhausted:
		return "retry_exhausted"
	default:
		return "canceled"
	}
}

// DropHandler 在数据被丢弃时回调，携带可区分的原因。
type DropHandler func(b Batch, reason DropReason)

// Dispatcher 提供有界队列、背压与重试。
// 队列容量固定：导出慢时采集侧在队列满后直接丢弃并回调原因，不无界堆积。
type Dispatcher struct {
	exp     Exporter
	onDrop  DropHandler
	retries int
	backoff time.Duration

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
		queue:   make(chan Batch, queueSize),
		done:    make(chan struct{}),
	}
}

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
// 重试针对同一 Batch.ID 进行，接收端可按 ID 去重，不产生重复可见数据。
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
	attempts := d.retries + 1
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			d.onDrop(b, DropCanceled)
			return
		}
		if err := d.exp.Export(ctx, b); err == nil {
			return
		}
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
