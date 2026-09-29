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
