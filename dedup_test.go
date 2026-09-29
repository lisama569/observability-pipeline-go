package pipeline

import (
	"context"
	"sync"
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
