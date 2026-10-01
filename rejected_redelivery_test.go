package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// waitOutcome 轮询去重器直到指定 ID 形成预期终态或超时，
// 用终态而不是 sleep 来同步，保证用例稳定可复现。
func waitOutcome(t *testing.T, d *Dispatcher, id uint64, want DedupOutcome) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := d.dedup.OutcomeOf(id); ok && got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("id=%d 在超时前未形成终态 %v", id, want)
}

// scriptedReceiver 按 Batch.ID 返回预置的前若干次错误，之后一律成功并计数。
// 用于构造“第一次取消/临时失败，之后重新投递成功”的确定性场景。
type scriptedReceiver struct {
	mu     sync.Mutex
	script map[uint64][]error
	seen   map[uint64]int
}

func (r *scriptedReceiver) Export(ctx context.Context, b Batch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if errs := r.script[b.ID]; len(errs) > 0 {
		err := errs[0]
		r.script[b.ID] = errs[1:]
		return err
	}
	if r.seen == nil {
		r.seen = map[uint64]int{}
	}
	r.seen[b.ID]++
	return nil
}

func (r *scriptedReceiver) arrivals(id uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[id]
}

// TestRejectedBatchRedeliverySuppressed 接收端明确拒收后，同一批次后续无论
// 再投递多少次，都不再打到接收端，且 permanently_rejected 丢弃只上报一次。
func TestRejectedBatchRedeliverySuppressed(t *testing.T) {
	recv := &rejectingReceiver{permanent: true}
	drops := &dropLog{}
	d := NewDispatcher(recv, 16, 3, drops.handler(), WithDedupCap(64))
	d.Start(context.Background())

	const id uint64 = 200
	d.Submit(Batch{ID: id, Records: []Record{{Message: "doomed"}}})
	waitOutcome(t, d, id, OutcomeRejected)

	// 首次拒收之后再投递同一批次：不应再触达接收端。
	for i := 0; i < 4; i++ {
		d.Submit(Batch{ID: id, Records: []Record{{Message: "doomed-redeliver"}}})
	}
	d.Close()

	t.Logf("拒收后重投: receiver attempts=%d drops=%v", recv.attempts.Load(), drops.reasons)
	if got := recv.attempts.Load(); got != 1 {
		t.Errorf("被拒收批次重投不应再打到接收端, 实际到达 %d 次, want 1", got)
	}
	if drops.count(DropPermanentlyRejected) != 1 {
		t.Errorf("同一批次的丢弃只应上报一次, got permanently_rejected=%d (%v)",
			drops.count(DropPermanentlyRejected), drops.reasons)
	}
	for _, other := range []DropReason{DropRetryExhausted, DropQueueFull, DropCanceled} {
		if drops.count(other) != 0 {
			t.Errorf("拒收批次的静默重投不得报 %s, got %d", other, drops.count(other))
		}
	}
}

// TestConcurrentRejectedSubmissionSeenOnce 主导者导出期间并发重复投递同一批次，
// 即使最终结论是永久拒收，接收端也只能被调用一次，丢弃也只上报一次。
func TestConcurrentRejectedSubmissionSeenOnce(t *testing.T) {
	recv := &rejectingReceiver{permanent: true, block: make(chan struct{})}
	release := recv.block // 主导者在接收端内部阻塞，制造并发窗口
	drops := &dropLog{}
	d := NewDispatcher(recv, 128, 0, drops.handler(), WithDedupCap(64))
	d.Start(context.Background())

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			d.Submit(Batch{ID: 300, Records: []Record{{Message: "c"}}})
		}()
	}
	wg.Wait() // 所有并发投递均已入队（主导者仍被阻塞）
	close(release)
	d.Close()

	if got := recv.attempts.Load(); got != 1 {
		t.Errorf("并发拒收批次接收端可见 %d 次, want 1", got)
	}
	if drops.count(DropPermanentlyRejected) != 1 {
		t.Errorf("并发拒收只应上报一次丢弃, got %v", drops.reasons)
	}
	if drops.count(DropRetryExhausted)+drops.count(DropQueueFull)+drops.count(DropCanceled) != 0 {
		t.Errorf("并发重复批次不得产生其他丢弃, got %v", drops.reasons)
	}
}

// TestTemporaryFailureRedelivered 临时失败重试耗尽只是“未决”：
// 之后重新投递同一批次必须正常送出，不能被当成已处理吞掉。
func TestTemporaryFailureRedelivered(t *testing.T) {
	recv := &scriptedReceiver{
		script: map[uint64][]error{400: {errors.New("temporary glitch")}},
		seen:   map[uint64]int{},
	}
	drops := &dropLog{}
	d := NewDispatcher(recv, 8, 0, drops.handler(), WithDedupCap(64)) // retries=0：首败即耗尽
	d.Start(context.Background())

	d.Submit(Batch{ID: 400, Records: []Record{{Message: "a"}}})
	waitOutcomeInflight(t, d)
	// 重新投递：接收端恢复，应正常送达一次。
	d.Submit(Batch{ID: 400, Records: []Record{{Message: "a-retry"}}})
	d.Close()

	if got := recv.arrivals(400); got != 1 {
		t.Errorf("临时失败后重投应正常送达一次, got %d", got)
	}
	if drops.count(DropRetryExhausted) != 1 {
		t.Errorf("首次投递应记一次 retry_exhausted, got %v", drops.reasons)
	}
	if drops.count(DropPermanentlyRejected) != 0 {
		t.Errorf("临时失败不得记为永久拒收, got %v", drops.reasons)
	}
}

// TestCanceledOrTimeoutBatchRedelivered 因取消/超时没送出去的批次同样是未决，
// 之后重新投递要正常送出，不被去重抑制。
func TestCanceledOrTimeoutBatchRedelivered(t *testing.T) {
	for _, firstErr := range []error{
		fmt.Errorf("export stopped: %w", context.Canceled),
		fmt.Errorf("export slow: %w", context.DeadlineExceeded),
	} {
		t.Run(firstErr.Error(), func(t *testing.T) {
			recv := &scriptedReceiver{
				script: map[uint64][]error{500: {firstErr}},
				seen:   map[uint64]int{},
			}
			drops := &dropLog{}
			d := NewDispatcher(recv, 8, 5, drops.handler(), WithDedupCap(64))
			d.Start(context.Background())

			d.Submit(Batch{ID: 500, Records: []Record{{Message: "late"}}})
			waitOutcomeInflight(t, d)
			d.Submit(Batch{ID: 500, Records: []Record{{Message: "late-redeliver"}}})
			d.Close()

			if got := recv.arrivals(500); got != 1 {
				t.Errorf("取消/超时后重投应正常送达一次, got %d", got)
			}
			if drops.count(DropCanceled) != 1 {
				t.Errorf("首次投递应记一次 canceled, got %v", drops.reasons)
			}
			if drops.count(DropPermanentlyRejected) != 0 {
				t.Errorf("取消/超时不得记为永久拒收, got %v", drops.reasons)
			}
		})
	}
}

// waitOutcomeInflight 等待队列中所有批次处理完毕（终态或丢弃均已落账）。
func waitOutcomeInflight(t *testing.T, d *Dispatcher) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d.Inflight() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("在途批次超时未完成")
}

// TestNewDispatcherLegacySignatureCompiles 老代码按历史位置参数
// (exp, queueSize, retries, onDrop) 调用构造入口必须照常编译与运行，
// 新增能力只通过可选项提供。
func TestNewDispatcherLegacySignatureCompiles(t *testing.T) {
	// 编译期：锁定构造入口的位置参数签名向后兼容。
	var legacy func(Exporter, int, int, DropHandler, ...DispatcherOption) *Dispatcher = NewDispatcher

	recv := &countingReceiver{}
	drops := &dropLog{}
	d := legacy(recv, 8, 1, drops.handler()) // 不传任何选项：去重仍以默认容量启用
	d.Start(context.Background())
	d.Submit(Batch{ID: 600, Records: []Record{{Message: "legacy"}}})
	d.Submit(Batch{ID: 600, Records: []Record{{Message: "legacy-dup"}}})
	d.Close()

	if got := recv.arrivals(600); got != 1 {
		t.Errorf("默认配置下成功去重仍应生效, 接收端可见 %d 次, want 1", got)
	}
	if len(drops.reasons) != 0 {
		t.Errorf("老签名默认行为不应产生丢弃, got %v", drops.reasons)
	}
}

// TestDeduperRejectedRemembered 拒收 ID 与送达 ID 一样被记住：
// 后续 Begin 为 follower、IsSettled 命中、结论可区分。
func TestDeduperRejectedRemembered(t *testing.T) {
	dd := NewDeduper(8)
	leader, finish := dd.Begin(1)
	if !leader {
		t.Fatal("首次提交应为 leader")
	}
	finish(OutcomeRejected)
	if !dd.IsSettled(1) {
		t.Error("拒收 ID 应被记住（IsSettled 命中）")
	}
	if got, ok := dd.OutcomeOf(1); !ok || got != OutcomeRejected {
		t.Errorf("OutcomeOf = (%v,%v), want (rejected,true)", got, ok)
	}
	if leader, _ := dd.Begin(1); leader {
		t.Error("拒收批次重新 Begin 应为 follower（静默抑制）")
	}
	if dd.Len() != 1 {
		t.Errorf("Len=%d, want 1", dd.Len())
	}
}

// TestDeduperRejectedSharesLRU 拒收与送达终态共用一条 LRU：
// 容量淘汰对两种结论一致生效，重复提交会刷新位置；被淘汰后重新投递视为新批次。
func TestDeduperRejectedSharesLRU(t *testing.T) {
	dd := NewDeduper(2)

	leader1, finish1 := dd.Begin(1)
	leader2, finish2 := dd.Begin(2)
	leader3, finish3 := dd.Begin(3)
	finish1(OutcomeRejected) // 1 被拒收
	finish2(OutcomeDelivered)
	if leader1 == false || leader2 == false || leader3 == false {
		t.Fatal("首次提交均应为 leader")
	}
	// 触碰 id=1（拒收后的重复投递），把它提升为最近使用。
	if leader, _ := dd.Begin(1); leader {
		t.Fatal("id=1 已拒收, 重复提交应为 follower")
	}
	finish3(OutcomeDelivered) // 容量 2，最久未使用的 id=2 被淘汰，id=1 保留

	if got, ok := dd.OutcomeOf(1); !ok || got != OutcomeRejected {
		t.Errorf("id=1 拒收结论应保留, got (%v,%v)", got, ok)
	}
	if _, ok := dd.OutcomeOf(2); ok {
		t.Error("id=2 应已被 LRU 淘汰")
	}
	if got, ok := dd.OutcomeOf(3); !ok || got != OutcomeDelivered {
		t.Errorf("id=3 应为已送达, got (%v,%v)", got, ok)
	}
	// 被淘汰的 id=2 再次提交视为新批次，拒收抑制不再作用于它。
	if leader, _ := dd.Begin(2); !leader {
		t.Error("被淘汰 ID 重新提交应为 leader（有界去重的明确取舍）")
	}
}
