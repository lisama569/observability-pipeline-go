package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// receiver 按 Batch.ID 去重，模拟“重复可见数据”检测。
type receiver struct {
	mu      sync.Mutex
	seen    map[uint64]int
	records []Record
	delay   time.Duration
	fail    int // 剩余失败次数
}

func (r *receiver) Export(ctx context.Context, b Batch) error {
	if r.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.delay):
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail > 0 {
		r.fail--
		return errors.New("temporary export failure")
	}
	r.seen[b.ID]++
	r.records = append(r.records, b.Records...)
	return nil
}

func (r *receiver) dups() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for id, c := range r.seen {
		if c > 1 {
			t := c - 1
			n += t
			_ = id
		}
	}
	return n
}

type dropLog struct {
	mu      sync.Mutex
	reasons map[DropReason]int
}

func (d *dropLog) handler() DropHandler {
	d.reasons = map[DropReason]int{}
	return func(b Batch, r DropReason) {
		d.mu.Lock()
		d.reasons[r]++
		d.mu.Unlock()
	}
}

func (d *dropLog) count(r DropReason) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reasons[r]
}

func TestBackpressureQueueFull(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}, delay: 20 * time.Millisecond}
	drops := &dropLog{}
	d := NewDispatcher(recv, 2, 0, drops.handler())
	ctx := context.Background()
	d.Start(ctx)

	const n = 20
	for i := 0; i < n; i++ {
		d.Submit(Batch{ID: uint64(i + 1), Records: []Record{{Message: "m"}}})
	}
	d.Close()
	t.Logf("queue=2 delay=20ms submit=%d exported=%d dropped(queue_full)=%d",
		n, len(recv.seen), drops.count(DropQueueFull))
	if drops.count(DropQueueFull) == 0 {
		t.Error("慢速导出端应触发背压丢弃")
	}
	if got := len(recv.seen) + drops.count(DropQueueFull); got != n {
		t.Errorf("exported(%d)+dropped(%d)=%d, want %d（不得静默丢失）",
			len(recv.seen), drops.count(DropQueueFull), got, n)
	}
	if recv.dups() != 0 {
		t.Errorf("接收端出现 %d 条重复数据", recv.dups())
	}
}

func TestRetryNoDuplicateVisible(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}, fail: 2} // 前 2 次失败
	drops := &dropLog{}
	d := NewDispatcher(recv, 8, 3, drops.handler())
	d.Start(context.Background())
	d.Submit(Batch{ID: 1, Records: []Record{{Message: "a"}}})
	d.Close()
	t.Logf("fail=2 retries=3: exported=%d dropped=%d dups=%d",
		len(recv.seen), drops.count(DropRetryExhausted), recv.dups())
	if recv.seen[1] != 1 {
		t.Errorf("批次应恰好可见一次, got %d", recv.seen[1])
	}
	if drops.count(DropRetryExhausted) != 0 {
		t.Error("重试预算内不应丢弃")
	}
}

func TestRetryExhaustedDrop(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}, fail: 100} // 永远失败
	drops := &dropLog{}
	d := NewDispatcher(recv, 8, 2, drops.handler())
	d.Start(context.Background())
	d.Submit(Batch{ID: 1, Records: []Record{{Message: "a"}}})
	d.Close()
	t.Logf("fail=∞ retries=2: exported=%d dropped(retry_exhausted)=%d",
		len(recv.seen), drops.count(DropRetryExhausted))
	if drops.count(DropRetryExhausted) != 1 {
		t.Errorf("want 1 retry_exhausted drop, got %d", drops.count(DropRetryExhausted))
	}
	if len(recv.seen) != 0 {
		t.Error("失败批次不得出现在接收端")
	}
}

func TestSubmitAfterClose(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}}
	drops := &dropLog{}
	d := NewDispatcher(recv, 1, 0, drops.handler())
	d.Start(context.Background())
	d.Close()
	d.Submit(Batch{ID: 1})
	t.Logf("closed 后投递: dropped(canceled)=%d", drops.count(DropCanceled))
	if drops.count(DropCanceled) != 1 {
		t.Errorf("关闭后投递应以 canceled 原因丢弃, got %d", drops.count(DropCanceled))
	}
}

// permExporter 每次调用都返回永久失败（接收端明确拒绝）。
type permExporter struct {
	mu    sync.Mutex
	calls int
}

func (e *permExporter) Export(ctx context.Context, b Batch) error {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	return Permanent(errors.New("schema rejected: unknown metric"))
}

func (e *permExporter) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// 自定义实现永久失败标记方法，验证不依赖 Permanent 包装也能识别。
type markerPermExporter struct{}

type markerPermErr struct{}

func (markerPermErr) Error() string     { return "bad request" }
func (markerPermErr) PermanentFailure() {}
func (markerPermExporter) Export(context.Context, Batch) error {
	return markerPermErr{}
}

// TestPermanentFailureNoRetryAndDistinctReason：永久失败必须立刻停止重试，
// 并且以与重试耗尽/队列满/取消不同的原因上报。
func TestPermanentFailureNoRetryAndDistinctReason(t *testing.T) {
	exp := &permExporter{}
	drops := &dropLog{}
	d := NewDispatcher(exp, 8, 100, drops.handler()) // 重试预算给得很大也不该用
	d.Start(context.Background())
	d.Submit(Batch{ID: 1, Records: []Record{{Message: "rejected"}}})
	d.Close()

	t.Logf("永久失败: export 调用=%d drops=%v", exp.callCount(), drops.reasons)
	if got := exp.callCount(); got != 1 {
		t.Errorf("永久失败不应重试, 实际导出调用 %d 次", got)
	}
	if drops.count(DropPermanent) != 1 {
		t.Errorf("want 1 permanent_rejected drop, got %d", drops.count(DropPermanent))
	}
	for _, r := range []DropReason{DropRetryExhausted, DropQueueFull, DropCanceled} {
		if drops.count(r) != 0 {
			t.Errorf("永久失败不得报为 %s, got %d", r, drops.count(r))
		}
	}
	if got := DropPermanent.String(); got != "permanent_rejected" {
		t.Errorf("DropPermanent.String()=%q", got)
	}
}

func TestPermanentFailureByMarkerInterface(t *testing.T) {
	if !IsPermanentFailure(markerPermErr{}) {
		t.Error("实现 PermanentFailure() 的错误应识别为永久失败")
	}
	if !IsPermanentFailure(fmt.Errorf("wrap: %w", markerPermErr{})) {
		t.Error("被 %%w 包装的永久失败错误仍应被识别")
	}
	drops := &dropLog{}
	d := NewDispatcher(markerPermExporter{}, 4, 5, drops.handler())
	d.Start(context.Background())
	d.Submit(Batch{ID: 9})
	d.Close()
	if drops.count(DropPermanent) != 1 {
		t.Errorf("标记方法方式: want 1 permanent drop, got %v", drops.reasons)
	}
}

func TestPermanentFailureClassification(t *testing.T) {
	if IsPermanentFailure(nil) {
		t.Error("nil 不是永久失败")
	}
	if IsPermanentFailure(errors.New("plain")) {
		t.Error("普通错误是暂时性失败")
	}
	perm := errors.New("no such dataset")
	if !IsPermanentFailure(Permanent(perm)) {
		t.Error("Permanent 包装应识别为永久失败")
	}
	if !errors.Is(Permanent(perm), perm) {
		t.Error("Permanent 应保留底层原因（errors.Is）")
	}
	// 取消/超时不属于永久失败，避免抢占取消丢弃原因。
	if IsPermanentFailure(context.Canceled) || IsPermanentFailure(context.DeadlineExceeded) {
		t.Error("取消/超时不得归类为永久失败")
	}
}

// TestTransientFailureStillRetriesToExhaustion：暂时性失败行为不回归——
// 照旧退避重试，预算用尽才丢，原因是 retry_exhausted 而不是 permanent_rejected。
func TestTransientFailureStillRetriesToExhaustion(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}, fail: 100}
	drops := &dropLog{}
	d := NewDispatcher(recv, 8, 2, drops.handler())
	d.Start(context.Background())
	d.Submit(Batch{ID: 1})
	d.Close()
	if drops.count(DropRetryExhausted) != 1 {
		t.Errorf("暂时性失败耗尽应报 retry_exhausted, got %v", drops.reasons)
	}
	if drops.count(DropPermanent) != 0 {
		t.Errorf("暂时性失败不得报 permanent, got %d", drops.count(DropPermanent))
	}
}

// TestDuplicateSubmissionSuppressed：同一批次 ID 即使重复投递多次，
// 在接收端也只出现一次，后续重复完全不触达接收端。
func TestDuplicateSubmissionSuppressed(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}}
	drops := &dropLog{}
	d := NewDispatcher(recv, 16, 0, drops.handler())
	d.Start(context.Background())
	for i := 0; i < 5; i++ {
		d.Submit(Batch{ID: 42, Records: []Record{{Message: "once"}}})
	}
	d.Close()
	t.Logf("重复投递 5 次: 接收端 seen=%d drops=%v", recv.seen[42], drops.reasons)
	if recv.seen[42] != 1 {
		t.Errorf("批次 42 应只可见一次, got %d", recv.seen[42])
	}
	if len(drops.reasons) != 0 {
		t.Errorf("幂等重复不应触发丢弃回调, got %v", drops.reasons)
	}
}

// TestRetryFailureThenRedeliverySucceeds：暂时性失败耗尽后保留再试机会，
// 同一 ID 重新投递应被当作新尝试（不被错误去重）。
func TestRetryFailureThenRedeliverySucceeds(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}, fail: 100}
	drops := &dropLog{}
	d := NewDispatcher(recv, 16, 1, drops.handler())
	d.Start(context.Background())
	d.Submit(Batch{ID: 7, Records: []Record{{Message: "later"}}})
	d.Close()
	if drops.count(DropRetryExhausted) != 1 {
		t.Fatalf("前置批次应重试耗尽, got %v", drops.reasons)
	}

	// 接收端恢复；同一 ID 重新投递。
	recv.mu.Lock()
	recv.fail = 0
	recv.mu.Unlock()
	d2 := NewDispatcher(recv, 16, 1, drops.handler())
	d2.Start(context.Background())
	d2.Submit(Batch{ID: 7, Records: []Record{{Message: "later"}}})
	d2.Close()
	if recv.seen[7] != 1 {
		t.Errorf("暂时性失败后重投应成功且只可见一次, got %d", recv.seen[7])
	}
	if drops.count(DropPermanent) != 0 {
		t.Errorf("暂时性失败不得留下永久标记, got %d", drops.count(DropPermanent))
	}
}

// TestPermanentDuplicateSuppressedWithoutDoubleDrop：永久失败后再次投递同 ID，
// 应被去重抑制，且不会重复上报 permanent 丢弃。
func TestPermanentDuplicateSuppressedWithoutDoubleDrop(t *testing.T) {
	exp := &permExporter{}
	drops := &dropLog{}
	d := NewDispatcher(exp, 16, 2, drops.handler())
	d.Start(context.Background())
	d.Submit(Batch{ID: 5})
	d.Submit(Batch{ID: 5})
	d.Close()
	if exp.callCount() != 1 {
		t.Errorf("永久失败批次的重复投递不得再触达接收端, calls=%d", exp.callCount())
	}
	if drops.count(DropPermanent) != 1 {
		t.Errorf("永久失败只应上报一次, got %d", drops.count(DropPermanent))
	}
}
