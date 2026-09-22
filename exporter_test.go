package pipeline

import (
	"context"
	"errors"
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
