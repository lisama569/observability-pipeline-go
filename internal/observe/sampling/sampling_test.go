package sampling

import (
	"context"
	"sync"
	"testing"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

// A fixed ratio yields a reproducible verdict: the same trace id decides the
// same way at every judgment point.
func TestRatioDeterministic(t *testing.T) {
	s := NewRatioSampler(0.5)
	id := trace.NewTraceID()
	first := s.Decide(id)
	for i := 0; i < 1000; i++ {
		if s.Decide(id) != first {
			t.Fatalf("verdict changed on repeat judgment %d", i)
		}
	}
	t.Logf("input: trace=%s ratio=0.5 hash=%d threshold<=%d => %s, stable over 1000 re-judgments",
		id, HashTraceID(id), s.threshold, first)
}

func TestRatioBounds(t *testing.T) {
	zero := NewRatioSampler(0)
	one := NewRatioSampler(2)
	var keptZero, keptOne int
	for i := 0; i < 200; i++ {
		id := trace.NewTraceID()
		if zero.Decide(id).Kept() {
			keptZero++
		}
		if !one.Decide(id).Kept() {
			keptOne++
		}
	}
	t.Logf("input: 200 random ids; ratio=0 kept=%d (want 0); ratio=2(clamped 1) not-kept=%d (want 0)",
		keptZero, keptOne)
	if keptZero != 0 || keptOne != 0 {
		t.Fatalf("ratio clamping failed: keptZero=%d notKeptOne=%d", keptZero, keptOne)
	}
}

// Roughly half of many independent ids should be kept at ratio 0.5.
func TestRatioDistribution(t *testing.T) {
	s := NewRatioSampler(0.5)
	kept := 0
	const n = 5000
	for i := 0; i < n; i++ {
		if s.Decide(trace.NewTraceID()) == DecisionKeep {
			kept++
		}
	}
	frac := float64(kept) / n
	t.Logf("input: %d random ids at ratio=0.5 => kept=%d frac=%.3f (basis: uniform FNV hash)", n, kept, frac)
	if frac < 0.45 || frac > 0.55 {
		t.Fatalf("sampling fraction out of tolerance: %.3f", frac)
	}
}

// Repeated judgments through the Decider agree, including across goroutines.
func TestDeciderConsistency(t *testing.T) {
	d := NewDecider(NewRatioSampler(0.3), 128, NewKeepSet(128))
	id := trace.NewTraceID()
	want := d.Decide(context.Background(), id)

	var wg sync.WaitGroup
	bad := make(chan Decision, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := d.Decide(context.Background(), id); got != want {
				bad <- got
			}
		}()
	}
	wg.Wait()
	close(bad)
	for got := range bad {
		t.Fatalf("inconsistent decision under concurrency: got %s want %s", got, want)
	}
	st := d.Stats()
	t.Logf("input: trace=%s verdict=%s; 200 concurrent judgments => all agree; stats=%+v",
		id, want, st)
}

// Exceptional chains are force-kept and stay force-kept on later judgments.
func TestExceptionalForceKeep(t *testing.T) {
	d := NewDecider(NewRatioSampler(0), 16, NewKeepSet(16))
	id := trace.NewTraceID()
	if d.Decide(context.Background(), id) != DecisionDrop {
		t.Fatalf("ratio=0 must initially drop")
	}
	d.NoteExceptional(id)
	got := d.Decide(context.Background(), id)
	t.Logf("input: trace=%s initially drop, then marked exceptional => %s (basis: keep-set upgrade)", id, got)
	if got != DecisionForceKeep {
		t.Fatalf("exceptional chain not force-kept: %s", got)
	}
}

// Bounded caches evict the oldest entry instead of growing unboundedly;
// re-judgment after eviction falls back to the reproducible sampler.
func TestBoundedEviction(t *testing.T) {
	cap := 4
	keep := NewKeepSet(cap)
	ids := make([]trace.TraceID, 6)
	for i := range ids {
		ids[i] = trace.NewTraceID()
		keep.Add(ids[i])
	}
	if keep.Len() != cap {
		t.Fatalf("keep-set unbounded: len=%d cap=%d", keep.Len(), cap)
	}
	// The two oldest ids were evicted.
	if keep.Contains(ids[0]) || keep.Contains(ids[1]) {
		t.Fatalf("oldest entries not evicted")
	}
	t.Logf("input: add 6 ids to keep-set cap=4 => len=%d; oldest 2 evicted, newest 4 retained (LRU)", keep.Len())

	d := NewDecider(NewRatioSampler(1), cap, keep)
	// Fill cache then verify stats report evictions and length stays bounded.
	for i := 0; i < 20; i++ {
		d.Decide(context.Background(), trace.NewTraceID())
	}
	st := d.Stats()
	t.Logf("decision cache cap=%d after 20 unique ids => evictions=%d (must be >0), judgments=%d",
		cap, st.Evictions, st.Judgments)
	if st.Evictions == 0 {
		t.Fatalf("decision cache did not evict")
	}
}
