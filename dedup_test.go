package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingReceiver 记录每个 Batch.ID 实际到达接收端的次数，可阻塞用于并发场景。
type countingReceiver struct {
	mu     sync.Mutex
	counts map[uint64]int
	block  map[uint64]chan struct{}
}

func (r *countingReceiver) Export(ctx context.Context, b Batch) error {
	if r.block != nil {
		if ch := r.block[b.ID]; ch != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = map[uint64]int{}
	}
	r.counts[b.ID]++
	return nil
}

func (r *countingReceiver) arrivals(id uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[id]
}

// TestRedeliverySeenOnce 同一批次成功送达后再次投递（含重试式重复），
// 接收端最终只可见一次，且重复投递不算丢弃。
func TestRedeliverySeenOnce(t *testing.T) {
	recv := &countingReceiver{}
	drops := &dropLog{}
	d := NewDispatcher(recv, 16, 0, 64, drops.handler())
	d.Start(context.Background())
	for i := 0; i < 5; i++ {
		d.Submit(Batch{ID: 77, Records: []Record{{Message: "x"}}})
	}
	d.Close()
	if got := recv.arrivals(77); got != 1 {
		t.Errorf("同批重复投递 5 次, 接收端可见 %d 次, want 1", got)
	}
	if len(drops.reasons) != 0 {
		t.Errorf("幂等重复不应触发丢弃, got %v", drops.reasons)
	}
}

// TestConcurrentSubmitSeenOnce 同一批次被并发提交多次（模拟并发重试/重复投递），
// 接收端恰好可见一次。
func TestConcurrentSubmitSeenOnce(t *testing.T) {
	recv := &countingReceiver{block: make(map[uint64]chan struct{})}
	release := make(chan struct{})
	recv.block[5] = release // 主导者导出期间阻塞，制造并发窗口
	drops := &dropLog{}
	d := NewDispatcher(recv, 128, 0, 64, drops.handler())
	d.Start(context.Background())

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			d.Submit(Batch{ID: 5, Records: []Record{{Message: "c"}}})
		}()
	}
	// 等待重复批次在队列里堆积，确保主导者尚未完成时并发投递已经发生。
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	d.Close()

	if got := recv.arrivals(5); got != 1 {
		t.Errorf("并发投递 %d 次, 接收端可见 %d 次, want 1", n, got)
	}
	if len(drops.reasons) != 0 {
		t.Errorf("并发幂等重复不应触发丢弃, got %v", drops.reasons)
	}
}

// TestDeduperLRUEviction 去重表必须有界：达到容量后按 LRU 淘汰最久未使用 ID，
// 被淘汰 ID 重新投递视为新批次。
func TestDeduperLRUEviction(t *testing.T) {
	const cap = 3
	dd := NewDeduper(cap)
	beginDelivered := func(id uint64) func(bool) {
		leader, finish := dd.Begin(id)
		if !leader {
			t.Fatalf("id=%d 首次提交应为 leader", id)
		}
		return finish
	}
	finish1 := beginDelivered(1)
	finish2 := beginDelivered(2)
	finish3 := beginDelivered(3)
	finish1(true)
	finish2(true)
	finish3(true)
	if dd.Len() != 3 {
		t.Fatalf("Len=%d, want 3", dd.Len())
	}
	// id=1 在再次提交中被触碰，应提升为最近使用。
	if leader, _ := dd.Begin(1); leader {
		t.Fatal("id=1 已送达, 重复提交应判定为非 leader")
	}
	// 插入 id=4，此时最久未使用的 id=2 被淘汰（1 刚被触碰）。
	leader4, finish4 := dd.Begin(4)
	if !leader4 {
		t.Fatal("id=4 为新批次, 应为 leader")
	}
	finish4(true)
	if dd.Len() != cap {
		t.Errorf("淘汰后 Len=%d, want %d", dd.Len(), cap)
	}
	if leader, _ := dd.Begin(2); !leader {
		t.Error("id=2 已被 LRU 淘汰, 重新提交应视为新批次（leader）")
	}
	if leader, _ := dd.Begin(1); leader {
		t.Error("id=1 最近使用, 不应被淘汰")
	}
	if leader, _ := dd.Begin(3); leader {
		t.Error("id=3 未被淘汰, 重复提交应判定为非 leader")
	}
}

// TestDeduperUndeliveredRetriable 未送达（临时失败/取消）不记入去重表，
// 后续重新投递应成为新的主导者。
func TestDeduperUndeliveredRetriable(t *testing.T) {
	dd := NewDeduper(4)
	leader, finish := dd.Begin(9)
	if !leader {
		t.Fatal("首次提交应为 leader")
	}
	finish(false)
	leader, _ = dd.Begin(9)
	if !leader {
		t.Error("未送达的批次不应进入去重表, 重新投递应为 leader")
	}
	if dd.Len() != 0 {
		t.Errorf("未送达不记录, Len=%d, want 0", dd.Len())
	}
}

// TestDeduperRejectedRecorded 被永久拒收的批次必须记为终态：
// 后续重复投递不再成为主导者（不会再打到接收端），且能取到拒收结论；
// 重复提交只刷新 LRU 位置，不新增条目。
func TestDeduperRejectedRecorded(t *testing.T) {
	dd := NewDeduper(8)
	leader, _, finish := dd.BeginEx(42)
	if !leader {
		t.Fatal("首次提交应为 leader")
	}
	finish(OutcomeRejected)
	if got, ok := dd.OutcomeOf(42); !ok || got != OutcomeRejected {
		t.Fatalf("拒收后 OutcomeOf=(%v,%v), want (%v,true)", got, ok, OutcomeRejected)
	}
	if dd.Len() != 1 {
		t.Fatalf("拒收应记录一条, Len=%d, want 1", dd.Len())
	}
	for i := 0; i < 3; i++ {
		leader, known, finish := dd.BeginEx(42)
		if leader {
			t.Fatalf("拒收批次第 %d 次重投不应再成为主导者", i+1)
		}
		if known != OutcomeRejected {
			t.Errorf("重复投递应取到拒收结论, got %v", known)
		}
		finish(OutcomeRejected) // 非主导者的 finish 必须是空操作
	}
	if dd.Len() != 1 {
		t.Errorf("重复投递不得新增条目, Len=%d, want 1", dd.Len())
	}
}

// TestDeduperRejectedVersusDelivered 已送达与已拒收结论互不混淆，
// 未送达结论绝不落表。
func TestDeduperRejectedVersusDelivered(t *testing.T) {
	dd := NewDeduper(8)
	_, _, f1 := dd.BeginEx(1)
	f1(OutcomeDelivered)
	_, _, f2 := dd.BeginEx(2)
	f2(OutcomeRejected)
	_, _, f3 := dd.BeginEx(3)
	f3(OutcomeUndelivered)

	if got, ok := dd.OutcomeOf(1); !ok || got != OutcomeDelivered {
		t.Errorf("id=1: (%v,%v), want delivered", got, ok)
	}
	if got, ok := dd.OutcomeOf(2); !ok || got != OutcomeRejected {
		t.Errorf("id=2: (%v,%v), want rejected", got, ok)
	}
	if _, ok := dd.OutcomeOf(3); ok {
		t.Error("id=3 未送达不应落表")
	}
	if dd.Len() != 2 {
		t.Errorf("Len=%d, want 2", dd.Len())
	}
}

// TestDeduperLRUEvictionMixedOutcomes 已送达与已拒收条目共用一张
// 有界 LRU，淘汰规则对两者一致；被淘汰的拒收 ID 重投视为新批次。
func TestDeduperLRUEvictionMixedOutcomes(t *testing.T) {
	const cap = 2
	dd := NewDeduper(cap)
	_, _, f1 := dd.BeginEx(1)
	f1(OutcomeDelivered)
	_, _, f2 := dd.BeginEx(2)
	f2(OutcomeRejected)
	// 触碰 id=1，插入 id=3 时应淘汰 id=2（拒收结论同样受容量约束）。
	if leader, _, _ := dd.BeginEx(1); leader {
		t.Fatal("id=1 已送达, 不应成为主导者")
	}
	leader, _, f3 := dd.BeginEx(3)
	if !leader {
		t.Fatal("id=3 为新批次, 应为 leader")
	}
	f3(OutcomeDelivered)
	if _, ok := dd.OutcomeOf(2); ok {
		t.Error("id=2 应已被 LRU 淘汰")
	}
	// 被淘汰的拒收 ID 重投成为新主导者——有界内存下的明确取舍。
	if leader, _, _ := dd.BeginEx(2); !leader {
		t.Error("已淘汰的拒收 ID 重投应视为新批次（leader）")
	}
}

// TestDeduperConcurrentFollowerTakesOver 主导者未送达（临时失败/取消）
// 时，并发等待者中必须有且仅有一个接管成为新主导者，重投不被吞掉；
// 主导者成功后所有等待者都只能是非主导者，接收端不会被打到第二次。
func TestDeduperConcurrentFollowerTakesOver(t *testing.T) {
	t.Run("leader_fails_one_follower_takes_over", func(t *testing.T) {
		dd := NewDeduper(8)
		leader, _, finishLeader := dd.BeginEx(7)
		if !leader {
			t.Fatal("首次提交应为 leader")
		}

		const followers = 8

		start := make(chan struct{})
		var leaders int64
		var wg sync.WaitGroup
		wg.Add(followers)
		for i := 0; i < followers; i++ {
			go func() {
				defer wg.Done()
				<-start
				l, _, f := dd.BeginEx(7)
				if l {
					atomic.AddInt64(&leaders, 1)
					f(OutcomeDelivered) // 接管者送达成功
				}
			}()
		}
		// 确保所有追随者都已在等待主导者。
		time.Sleep(20 * time.Millisecond)
		finishLeader(OutcomeUndelivered) // 主导者失败
		close(start)
		wg.Wait()

		if leaders != 1 {
			t.Errorf("主导者未送达时应有且仅有 1 个追随者接管, got %d", leaders)
		}
		if got, ok := dd.OutcomeOf(7); !ok || got != OutcomeDelivered {
			t.Errorf("接管者送达后应为 delivered, got (%v,%v)", got, ok)
		}
	})

	t.Run("leader_delivers_all_followers_suppressed", func(t *testing.T) {
		dd := NewDeduper(8)
		leader, _, finishLeader := dd.BeginEx(8)
		if !leader {
			t.Fatal("首次提交应为 leader")
		}

		const followers = 8

		start := make(chan struct{})
		var leaders int64
		var wg sync.WaitGroup
		wg.Add(followers)
		for i := 0; i < followers; i++ {
			go func() {
				defer wg.Done()
				<-start
				if l, _, _ := dd.BeginEx(8); l {
					atomic.AddInt64(&leaders, 1)
				}
			}()
		}
		time.Sleep(20 * time.Millisecond)
		finishLeader(OutcomeDelivered) // 主导者成功
		close(start)
		wg.Wait()

		if leaders != 0 {
			t.Errorf("主导者已送达, 追随者不应再成为主导者, got %d", leaders)
		}
	})
}
