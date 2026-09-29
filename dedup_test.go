package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// TestDedupConcurrentSingleflight：同一批次 ID 并发登记，恰好一个 leader；
// leader 只导出一次。其余调用方要么作为 follower 等待同一结论（并发重试合并），
// 要么在结论定案后直接被去重抑制——最终只产生一次可见到达。
func TestDedupConcurrentSingleflight(t *testing.T) {
	tbl := newDedupTable(64)
	const n = 64
	var leaders atomic.Int32
	var exports atomic.Int32
	var followers atomic.Int32
	var dups atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			a, outcome := tbl.begin(100)
			switch outcome {
			case outcomeLeader:
				leaders.Add(1)
				// 模拟实际导出：只应发生一次。
				exports.Add(1)
				tbl.finalize(100, a, nil)
			case outcomeFollower:
				followers.Add(1)
				finalized, err := a.wait(context.Background())
				if err != nil || !finalized {
					t.Errorf("follower %d: want finalized result, got %v %v", i, finalized, err)
				}
			case outcomeDup:
				dups.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := leaders.Load(); got != 1 {
		t.Errorf("leader 数=%d, want 1", got)
	}
	if got := exports.Load(); got != 1 {
		t.Errorf("实际导出次数=%d, want 1（并发重试合并）", got)
	}
	if followers.Load()+dups.Load() != n-1 {
		t.Errorf("其余调用方数=%d, want %d", followers.Load()+dups.Load(), n-1)
	}
	// 首轮全部结束后再投递同 ID，必须被终结去重直接抑制。
	for i := 0; i < 8; i++ {
		if _, outcome := tbl.begin(100); outcome != outcomeDup {
			t.Fatalf("终结后重复投递应返回 outcomeDup, got %d", outcome)
		}
	}
}

// TestDedupTransientFailureRerunAllowsRetry：leader 暂时性放弃（abandon）后，
// live 条目被清除，新的登记应重新成为 leader 并能成功——暂时性失败不被错误去重。
func TestDedupTransientFailureRerunAllowsRetry(t *testing.T) {
	tbl := newDedupTable(8)
	a, outcome := tbl.begin(7)
	if outcome != outcomeLeader {
		t.Fatalf("首次登记应为 leader, got %d", outcome)
	}
	tbl.abandon(7, a, errTemp{})

	a2, outcome2 := tbl.begin(7)
	if outcome2 != outcomeLeader {
		t.Fatalf("暂时性放弃后重新登记应为新 leader, got %d", outcome2)
	}
	tbl.finalize(7, a2, nil)
	if _, outcome3 := tbl.begin(7); outcome3 != outcomeDup {
		t.Fatalf("成功后应被去重, got %d", outcome3)
	}
}

// TestDedupFIFOEvictionBounded：去重记忆严格受容量约束，按终结顺序 FIFO 淘汰。
func TestDedupFIFOEvictionBounded(t *testing.T) {
	const cap = 3
	tbl := newDedupTable(cap)
	finish := func(id uint64) {
		a, o := tbl.begin(id)
		if o != outcomeLeader {
			t.Fatalf("id=%d 应成为 leader, got %d", id, o)
		}
		tbl.finalize(id, a, nil)
	}
	for id := uint64(1); id <= 5; id++ {
		finish(id)
	}
	tbl.mu.Lock()
	got := len(tbl.seen)
	ord := append([]uint64(nil), tbl.ord...)
	tbl.mu.Unlock()
	if got != cap {
		t.Fatalf("seen 大小=%d, want %d（容量有界）", got, cap)
	}
	want := []uint64{3, 4, 5}
	for i, id := range want {
		if ord[i] != id {
			t.Errorf("FIFO 顺序错位: %v, want %v", ord, want)
		}
	}
	// 1、2 已被淘汰：重新投递成为新尝试，而不是被抑制。
	if _, o := tbl.begin(1); o != outcomeLeader {
		t.Errorf("已淘汰 ID 重新投递应成为 leader, got %d", o)
	}
	// 3 仍在窗口内：重复被抑制。
	if _, o := tbl.begin(3); o != outcomeDup {
		t.Errorf("窗口内 ID 重复应被抑制, got %d", o)
	}
}

// TestDedupCapacityDefault：容量 <=0 时回落到默认值。
func TestDedupCapacityDefault(t *testing.T) {
	if newDedupTable(0).cap != DefaultDedupCap {
		t.Error("容量 0 应取默认值")
	}
	if newDedupTable(-5).cap != DefaultDedupCap {
		t.Error("负容量应取默认值")
	}
}

// TestDedupConcurrentPermanentOnlyOnce：并发场景下永久失败也只定案一次。
func TestDedupConcurrentPermanentOnlyOnce(t *testing.T) {
	tbl := newDedupTable(16)
	const n = 32
	var leaders atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			a, o := tbl.begin(99)
			if o == outcomeLeader {
				leaders.Add(1)
				tbl.finalize(99, a, Permanent(errTemp{}))
				return
			}
			if o == outcomeFollower {
				finalized, err := a.wait(context.Background())
				if err != nil || !finalized {
					t.Errorf("follower 应看到永久失败终结结论")
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if leaders.Load() != 1 {
		t.Errorf("永久失败 leader 数=%d, want 1", leaders.Load())
	}
	if _, o := tbl.begin(99); o != outcomeDup {
		t.Errorf("永久失败后重复应被抑制, got %d", o)
	}
}

type errTemp struct{}

func (errTemp) Error() string { return "temporary" }
