package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// rejectingReceiver 可按调用次数返回永久失败或临时失败。
type rejectingReceiver struct {
	mu        sync.Mutex
	seen      map[uint64]int
	attempts  atomic.Int64
	permanent bool
	block     chan struct{} // 非 nil 时阻塞导出直到该 channel 关闭
}

func (r *rejectingReceiver) Export(ctx context.Context, b Batch) error {
	r.attempts.Add(1)
	if r.block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.block:
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[uint64]int{}
	}
	if r.permanent {
		return MarkPermanent(errors.New("400 batch rejected"))
	}
	r.seen[b.ID]++
	return nil
}

// TestPermanentFailureNoRetry 永久失败必须立即停止，不重试、不退避等待，
// 丢弃原因与重试耗尽/队列满/取消明确区分。
func TestPermanentFailureNoRetry(t *testing.T) {
	recv := &rejectingReceiver{permanent: true}
	drops := &dropLog{}
	d := NewDispatcher(recv, 8, 5, 64, drops.handler())
	d.Start(context.Background())

	start := time.Now()
	d.Submit(Batch{ID: 100, Records: []Record{{Message: "bad"}}})
	d.Close()
	elapsed := time.Since(start)

	t.Logf("永久失败: attempts=%d drops=%v elapsed=%v",
		recv.attempts.Load(), drops.reasons, elapsed)
	if got := recv.attempts.Load(); got != 1 {
		t.Errorf("永久失败不应重试, 尝试次数=%d, want 1", got)
	}
	if drops.count(DropPermanentlyRejected) != 1 {
		t.Errorf("want 1 permanently_rejected drop, got %v", drops.reasons)
	}
	for _, other := range []DropReason{DropRetryExhausted, DropQueueFull, DropCanceled} {
		if drops.count(other) != 0 {
			t.Errorf("永久失败不得混入 %s, got %d", other, drops.count(other))
		}
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("永久失败应立即返回, 实际等待 %v（疑似仍走退避）", elapsed)
	}
	if DropPermanentlyRejected.String() == DropRetryExhausted.String() {
		t.Fatal("丢弃原因字符串必须可区分")
	}
}

// TestPipelinePermanentRejectEndToEnd 真实管线：接收端永久拒绝时，
// 批次以 permanently_rejected 上报，且不重试、后续正常批次不受影响。
func TestPipelinePermanentRejectEndToEnd(t *testing.T) {
	recv := &rejectingReceiver{permanent: true}
	drops := &dropLog{}
	p := NewPipeline(Config{
		SampleRate: 1, CacheCap: 4, QueueSize: 16, Retries: 3, DedupCap: 32, BatchSize: 1,
	}, recv, drops.handler())
	p.Start(context.Background())
	ctx := p.Begin(context.Background())
	p.Emit(ctx, LevelInfo, "doomed", nil)
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if recv.attempts.Load() != 1 {
		t.Errorf("永久失败批次尝试 %d 次, want 1", recv.attempts.Load())
	}
	if drops.count(DropPermanentlyRejected) != 1 {
		t.Errorf("want 1 permanently_rejected, got %v", drops.reasons)
	}
	if drops.count(DropRetryExhausted) != 0 {
		t.Errorf("永久失败不得报重试耗尽, got %v", drops.reasons)
	}
}

// TestPermanentFailureReasonDistinct 验证包装与判定规则。
func TestPermanentFailureReasonDistinct(t *testing.T) {
	base := errors.New("bad payload")
	permanent := fmt.Errorf("wrap: %w", MarkPermanent(base))
	if !IsPermanent(permanent) {
		t.Error("被包装的永久失败应可识别")
	}
	if IsPermanent(nil) {
		t.Error("nil 不是永久失败")
	}
	if IsPermanent(context.Canceled) || IsPermanent(context.DeadlineExceeded) {
		t.Error("取消/超时属于生命周期终止，不算永久失败")
	}
	if MarkPermanent(nil) != nil {
		t.Error("MarkPermanent(nil) 应返回 nil")
	}
	if MarkPermanent(MarkPermanent(base)) == nil {
		t.Error("重复包装不应丢失永久失败属性")
	}
	if !IsPermanent(MarkPermanent(MarkPermanent(base))) {
		t.Error("重复包装后仍应识别为永久失败")
	}
}

// waitFor 轮询条件成立，超时即失败，避免用固定 sleep 制造时序依赖。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待 %s 超时", what)
}

// TestPermanentRejectRedeliverySuppressed 被接收端明确拒收过的批次
// 再次（串行）投递时：不再打到接收端，也不再产生第二次丢弃上报。
func TestPermanentRejectRedeliverySuppressed(t *testing.T) {
	recv := &rejectingReceiver{permanent: true}
	drops := &dropLog{}
	d := NewDispatcher(recv, 16, 5, 64, drops.handler())
	d.Start(context.Background())

	const id = uint64(200)
	for i := 0; i < 4; i++ {
		d.Submit(Batch{ID: id, Records: []Record{{Message: "doomed"}}})
	}
	d.Close()

	t.Logf("拒收批次重投 4 次: receiver 尝试=%d drops=%v",
		recv.attempts.Load(), drops.reasons)
	if got := recv.attempts.Load(); got != 1 {
		t.Errorf("拒收批次只应打到接收端 1 次, got %d", got)
	}
	if got := drops.count(DropPermanentlyRejected); got != 1 {
		t.Errorf("同一批次的丢弃只应上报 1 次, got %d", got)
	}
	for _, other := range []DropReason{DropRetryExhausted, DropQueueFull, DropCanceled} {
		if drops.count(other) != 0 {
			t.Errorf("拒收抑制不得混入 %s, got %d", other, drops.count(other))
		}
	}
}

// TestPermanentRejectConcurrentSuppressed 同一拒收批次在主导者导出期间
// 被并发重复投递：接收端最终只被调用一次，丢弃只上报一次。
func TestPermanentRejectConcurrentSuppressed(t *testing.T) {
	gate := make(chan struct{})
	recv := &rejectingReceiver{permanent: true, block: gate}
	drops := &dropLog{}
	const n = 16
	d := NewDispatcher(recv, n+1, 5, 64, drops.handler())
	d.Start(context.Background())

	const id = uint64(201)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			d.Submit(Batch{ID: id, Records: []Record{{Message: "doomed"}}})
		}()
	}
	// 等所有重复批次进入队列（1 个在导出中阻塞，n-1 个排队），
	// 确保主导者尚未给出拒收结论时并发投递已经发生。
	waitFor(t, "全部重复批次被接收", func() bool { return d.Inflight() == n })
	close(gate)
	wg.Wait()
	d.Close()

	t.Logf("并发拒收: 提交=%d receiver 尝试=%d drops=%v",
		n, recv.attempts.Load(), drops.reasons)
	if got := recv.attempts.Load(); got != 1 {
		t.Errorf("并发拒收批次只应打到接收端 1 次, got %d", got)
	}
	if got := drops.count(DropPermanentlyRejected); got != 1 {
		t.Errorf("同一批次的丢弃只应上报 1 次, got %d", got)
	}
	if total := drops.count(DropQueueFull) + drops.count(DropRetryExhausted) +
		drops.count(DropCanceled); total != 0 {
		t.Errorf("并发抑制不得产生其他丢弃, got %v", drops.reasons)
	}
}

// TestTransientFailureRedelivered 只是暂时性失败、重试预算耗尽被丢弃的
// 批次不入去重表：之后重新投递同一 ID 必须正常送达，不能被当成处理过。
func TestTransientFailureRedelivered(t *testing.T) {
	// 前 3 次调用临时失败（retries=2 → 首轮恰好 3 次尝试后耗尽预算），
	// 之后的调用成功。
	recv := &receiver{seen: map[uint64]int{}, fail: 3}
	drops := &dropLog{}
	d := NewDispatcher(recv, 8, 2, 64, drops.handler())
	d.Start(context.Background())

	const id = uint64(202)
	d.Submit(Batch{ID: id, Records: []Record{{Message: "flaky"}}})
	waitFor(t, "首轮重试耗尽", func() bool { return drops.count(DropRetryExhausted) == 1 })
	if recv.seen[id] != 0 {
		t.Fatalf("耗尽前不应送达, seen=%d", recv.seen[id])
	}

	// 同一批次重新投递：临时失败结论必须被抑制，正常再走接收端。
	d.Submit(Batch{ID: id, Records: []Record{{Message: "flaky"}}})
	d.Close()

	if got := recv.seen[id]; got != 1 {
		t.Errorf("临时失败批次重投应恰好送达 1 次, got %d", got)
	}
	if got := drops.count(DropRetryExhausted); got != 1 {
		t.Errorf("重试耗尽只应上报 1 次, got %d", got)
	}
}

// saturatedExporter：permanentIDs 中的批次立即永久拒收；其余批次阻塞，
// 用于把队列填满，验证队列饱和时已拒收批次重投仍被静默抑制。
type saturatedExporter struct {
	mu           sync.Mutex
	attempts     atomic.Int64
	seen         map[uint64]int
	permanentIDs map[uint64]bool
	block        chan struct{}
}

func (e *saturatedExporter) Export(ctx context.Context, b Batch) error {
	e.attempts.Add(1)
	e.mu.Lock()
	permanent := e.permanentIDs[b.ID]
	e.mu.Unlock()
	if permanent {
		return MarkPermanent(errors.New("400 batch rejected"))
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.block:
	}
	e.mu.Lock()
	e.seen[b.ID]++
	e.mu.Unlock()
	return nil
}

// TestPermanentRejectRedeliveryWhileQueueFull 队列被慢导出占满时重投
// 已拒收批次：入队前即被抑制，既不会报 queue_full，也不会再报一次
// permanently_rejected（同一批次的丢弃上报始终只有一次）。
func TestPermanentRejectRedeliveryWhileQueueFull(t *testing.T) {
	const qsize = 2
	block := make(chan struct{})
	recv := &saturatedExporter{
		permanentIDs: map[uint64]bool{300: true},
		seen:         map[uint64]int{},
		block:        block,
	}
	drops := &dropLog{}
	d := NewDispatcher(recv, qsize, 0, 64, drops.handler())
	d.Start(context.Background())

	// 1. 先让拒收批次得到终态。
	d.Submit(Batch{ID: 300})
	waitFor(t, "拒收结论落表", func() bool { return drops.count(DropPermanentlyRejected) == 1 })

	// 2. 用慢批次把队列占满（1 个在导出中阻塞 + qsize 个排队）。
	for i := 0; i < qsize+1; i++ {
		d.Submit(Batch{ID: uint64(400 + i)})
	}
	waitFor(t, "队列饱和", func() bool { return d.Inflight() == qsize+1 })

	// 3. 队列满时重投已拒收批次：Submit 非阻塞，是否被抑制在其返回时
	// 已成定局（未抑制则会同义产生 queue_full），直接断言无需 sleep。
	d.Submit(Batch{ID: 300})
	d.Submit(Batch{ID: 300})
	if drops.count(DropQueueFull) != 0 {
		t.Errorf("终态批次重投不应占队列, queue_full=%d", drops.count(DropQueueFull))
	}
	if drops.count(DropPermanentlyRejected) != 1 {
		t.Errorf("拒收丢弃应只有 1 次, got %d", drops.count(DropPermanentlyRejected))
	}
	if got := d.Inflight(); got != qsize+1 {
		t.Errorf("被抑制批次不得进入队列, inflight=%d, want %d", got, qsize+1)
	}

	close(block)
	d.Close()
}

// lifecycleExporter 首次导出阻塞到 gate 关闭后返回 firstErr
// （模拟导出调用被取消/超时），此后同批次重投正常接收。
type lifecycleExporter struct {
	mu       sync.Mutex
	seen     map[uint64]int
	attempts atomic.Int64
	gate     chan struct{}
	firstErr error
}

func (e *lifecycleExporter) Export(ctx context.Context, b Batch) error {
	if e.attempts.Add(1) == 1 {
		<-e.gate
		return e.firstErr
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen[b.ID]++
	return nil
}

// TestCanceledBatchRedelivered 因取消/超时没送出去的批次不记终态：
// 之后重新投递同一 ID 必须正常到达接收端（分别覆盖 DeadlineExceeded
// 与被包装的 Canceled）。
func TestCanceledBatchRedelivered(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"deadline_exceeded", context.DeadlineExceeded},
		{"wrapped_canceled", fmt.Errorf("export stopped: %w", context.Canceled)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate := make(chan struct{})
			recv := &lifecycleExporter{gate: gate, firstErr: tc.err, seen: map[uint64]int{}}
			drops := &dropLog{}
			d := NewDispatcher(recv, 16, 3, 64, drops.handler())
			d.Start(context.Background())

			const id = uint64(203)
			d.Submit(Batch{ID: id, Records: []Record{{Message: "late"}}})
			waitFor(t, "主导者进入导出", func() bool { return recv.attempts.Load() == 1 })
			close(gate) // 导出以生命周期错误结束
			waitFor(t, "取消丢弃上报", func() bool { return d.Inflight() == 0 })
			if drops.count(DropCanceled) != 1 {
				t.Fatalf("want 1 canceled drop, got %v", drops.reasons)
			}

			// 同一批次重新投递：取消结论必须被抑制，接收端应正常可见。
			d.Submit(Batch{ID: id, Records: []Record{{Message: "late"}}})
			d.Close()

			if got := recv.seen[id]; got != 1 {
				t.Errorf("取消批次重投应恰好送达 1 次, got %d", got)
			}
			if got := recv.attempts.Load(); got != 2 {
				t.Errorf("接收端应被调用 2 次（首次取消 + 重投成功）, got %d", got)
			}
			if got := drops.count(DropCanceled); got != 1 {
				t.Errorf("取消只应上报 1 次, got %d", got)
			}
		})
	}
}
