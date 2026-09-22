package trace

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func collect() (*sync.Mutex, *[]Record, Sink) {
	var mu sync.Mutex
	var recs []Record
	sink := Sink(func(_ context.Context, r Record) {
		mu.Lock()
		recs = append(recs, r)
		mu.Unlock()
	})
	return &mu, &recs, sink
}

func snapshot(mu *sync.Mutex, recs *[]Record) []Record {
	mu.Lock()
	defer mu.Unlock()
	out := make([]Record, len(*recs))
	copy(out, *recs)
	return out
}

// Correlation propagates by value; concurrent dispatched goroutines must not
// cross-talk or lose their identity.
func TestConcurrentDetachNoCrosstalk(t *testing.T) {
	mu, recs, sink := collect()
	ctx := WithSink(context.Background(), sink)
	ctx, sp := StartSpan(ctx, "root")
	root := sp.Correlation()
	t.Logf("input: root trace=%s span=%s; spawning 200 detached goroutines each logging once (basis: correlation copied by value into each goroutine)",
		root.TraceID, root.SpanID)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		Detach(ctx, "worker", func(c context.Context) {
			defer wg.Done()
			cc, ok := CorrelationFrom(c)
			if !ok || cc.TraceID != root.TraceID || cc.SpanID != root.SpanID {
				t.Errorf("lost/changed correlation in detached goroutine")
				return
			}
			NewLogger(c).Info(c, "work", F("i", i))
		})
	}
	wg.Wait()

	got := snapshot(mu, recs)
	if len(got) != n {
		t.Fatalf("records lost: got %d want %d", len(got), n)
	}
	for _, r := range got {
		if r.TraceID != root.TraceID {
			t.Fatalf("cross-talk: trace id changed to %s", r.TraceID)
		}
		if r.SpanID != root.SpanID {
			t.Fatalf("cross-talk: span id changed to %s", r.SpanID)
		}
	}
	t.Logf("verdict: %d/%d records share exactly the root identity; no cross-talk", len(got), n)
}

// Records produced after cancellation are emitted and marked terminated,
// never silently dropped.
func TestCanceledChainMarkedNotDropped(t *testing.T) {
	mu, recs, sink := collect()
	ctx, cancel := context.WithCancel(WithSink(context.Background(), sink))
	ctx, sp := StartSpan(ctx, "op")
	corr := sp.Correlation()
	cancel()

	l := sp.Logger(ctx)
	l.Info(ctx, "after cancel")
	t.Logf("input: context canceled=%v, then emit one info record (basis: ctx.Err()==Canceled sets Terminated/TermReason)",
		ctx.Err() != nil)

	got := snapshot(mu, recs)
	if len(got) != 1 {
		t.Fatalf("record silently dropped or duplicated: got %d want 1", len(got))
	}
	r := got[0]
	if !r.Terminated || r.TermReason != TermCanceled {
		t.Fatalf("not marked terminated: %+v", r)
	}
	if r.TraceID != corr.TraceID {
		t.Fatalf("correlation lost after cancel")
	}
	t.Logf("verdict: record retained, terminated=%v reason=%s trace=%s",
		r.Terminated, r.TermReason, r.TraceID)
}

func TestDeadlineChainMarked(t *testing.T) {
	mu, recs, sink := collect()
	ctx, cancel := context.WithTimeout(WithSink(context.Background(), sink), time.Millisecond)
	defer cancel()
	ctx, sp := StartSpan(ctx, "op")
	time.Sleep(5 * time.Millisecond)
	sp.Logger(ctx).Info(ctx, "late")

	got := snapshot(mu, recs)
	if len(got) != 1 || !got[0].Terminated || got[0].TermReason != TermTimeout {
		t.Fatalf("expected one timeout-marked record, got %+v", got)
	}
	t.Logf("input: deadline elapsed; verdict: terminated=%v reason=%s",
		got[0].Terminated, got[0].TermReason)
}

// Detached work survives parent cancellation but observes the termination
// state of its originating chain and marks records accordingly.
func TestDetachObservesParentTermination(t *testing.T) {
	mu, recs, sink := collect()
	pctx, cancel := context.WithCancel(WithSink(context.Background(), sink))
	pctx, sp := StartSpan(pctx, "parent")
	cancel()

	done := make(chan struct{})
	Detach(pctx, "bg", func(c context.Context) {
		// The detached context itself must not be canceled...
		if c.Err() != nil {
			t.Errorf("detached context must stay alive")
		}
		sp.Logger(c).Info(c, "detached after parent cancel")
		close(done)
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("detached goroutine did not run")
	}

	got := snapshot(mu, recs)
	if len(got) != 1 || !got[0].Terminated || got[0].TermReason != TermCanceled {
		t.Fatalf("detached record should be marked parent-terminated, got %+v", got)
	}
	t.Logf("input: parent canceled before detached log; verdict: terminated=%v reason=%s while detached ctx alive",
		got[0].Terminated, got[0].TermReason)
}

func TestErrorClassifiedOnRecord(t *testing.T) {
	mu, recs, sink := collect()
	ctx := WithSink(context.Background(), sink)
	ctx, sp := StartSpan(ctx, "op")
	sp.Logger(ctx).Error(ctx, "failed", errors.New("boom"))

	r := snapshot(mu, recs)[0]
	t.Logf("input error=%q => error_kind=%q level=%s (basis: errkind.Classify; unknown=>internal)",
		"boom", r.ErrorKind, r.Level)
	if r.ErrorKind != "internal" {
		t.Fatalf("want internal, got %q", r.ErrorKind)
	}

	cctx, csp := StartSpan(WithSink(context.Background(), sink), "op2")
	cctx2, cancel := context.WithCancel(cctx)
	cancel()
	csp.Logger(cctx2).Error(cctx2, "canceled", context.Canceled)
	all := snapshot(mu, recs)
	r2 := all[1]
	t.Logf("input error=%q => error_kind=%q terminated=%v reason=%s (basis: canceled error marks chain terminated)",
		context.Canceled, r2.ErrorKind, r2.Terminated, r2.TermReason)
	if r2.ErrorKind != "canceled" || !r2.Terminated || r2.TermReason != TermCanceled {
		t.Fatalf("canceled error misclassified: %+v", r2)
	}
}

func TestSpanEndOnce(t *testing.T) {
	mu, recs, sink := collect()
	ctx, sp := StartSpan(WithSink(context.Background(), sink), "once")
	sp.End(ctx, errors.New("x"))
	sp.End(ctx, errors.New("y"))
	got := snapshot(mu, recs)
	if len(got) != 1 {
		t.Fatalf("End must be idempotent, got %d records", len(got))
	}
	t.Logf("input: End called twice; verdict: %d final record emitted", len(got))
}

func TestIDsRoundTrip(t *testing.T) {
	tid, sid := NewTraceID(), NewSpanID()
	tid2, err := ParseTraceID(tid.String())
	if err != nil || tid2 != tid {
		t.Fatalf("trace id round trip failed: %v", err)
	}
	sid2, err := ParseSpanID(sid.String())
	if err != nil || sid2 != sid {
		t.Fatalf("span id round trip failed: %v", err)
	}
	t.Logf("ids round trip ok: trace=%s span=%s", tid, sid)
}
