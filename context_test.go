package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCorrelationPropagationAcrossGoroutines(t *testing.T) {
	p := NewPipeline(Config{SampleRate: 1, BatchSize: 1, QueueSize: 64}, nil, nil)
	ctx := p.Begin(context.Background())
	base := CorrelationFrom(ctx)
	t.Logf("base correlation: trace=%d span=%d", base.TraceID, base.SpanID)

	const n = 32
	seen := make([]Correlation, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			child := p.Derive(ctx) // 并发派生子 Span
			seen[i] = CorrelationFrom(child)
		}(i)
	}
	wg.Wait()

	spans := map[uint64]bool{}
	for i, c := range seen {
		if c.TraceID != base.TraceID {
			t.Fatalf("goroutine %d: traceID=%d, want %d（串扰）", i, c.TraceID, base.TraceID)
		}
		if c.SpanID == base.SpanID || spans[c.SpanID] {
			t.Fatalf("goroutine %d: spanID=%d 冲突", i, c.SpanID)
		}
		spans[c.SpanID] = true
	}
	t.Logf("判定依据: %d 个协程共享 traceID=%d, spanID 全部唯一", n, base.TraceID)
}

func TestCanceledContextMarksTerminated(t *testing.T) {
	p := NewPipeline(Config{}, nil, nil)

	ctx, cancel := context.WithCancel(p.Begin(context.Background()))
	cancel()
	c := CorrelationFrom(ctx)
	t.Logf("canceled: terminated=%v reason=%q", c.Terminated, c.TermReason)
	if !c.Terminated || c.TermReason != "canceled" {
		t.Errorf("got terminated=%v reason=%q, want true/canceled", c.Terminated, c.TermReason)
	}

	ctx2, cancel2 := context.WithTimeout(p.Begin(context.Background()), time.Nanosecond)
	defer cancel2()
	time.Sleep(time.Millisecond)
	c2 := CorrelationFrom(ctx2)
	t.Logf("timeout: terminated=%v reason=%q", c2.Terminated, c2.TermReason)
	if !c2.Terminated || c2.TermReason != "timeout" {
		t.Errorf("got terminated=%v reason=%q, want true/timeout", c2.Terminated, c2.TermReason)
	}
}
