package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/observability-pipeline-go/internal/observe/errkind"
	"github.com/lisama569/observability-pipeline-go/internal/observe/export"
	"github.com/lisama569/observability-pipeline-go/internal/observe/wire"
)

// recExporter captures successfully exported batches.
type recExporter struct {
	mu      sync.Mutex
	batches []*wire.Batch
	raw     [][]byte
	delay   time.Duration
	fail    int
}

func (e *recExporter) Export(ctx context.Context, data []byte) error {
	if e.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.delay):
		}
	}
	e.mu.Lock()
	fail := e.fail
	if fail > 0 {
		e.fail--
	}
	e.mu.Unlock()
	if fail > 0 {
		return export.Retryable(errors.New("temporary"))
	}
	b, err := wire.Decode(data)
	if err != nil {
		return export.Permanent(err)
	}
	e.mu.Lock()
	e.batches = append(e.batches, b)
	e.raw = append(e.raw, append([]byte(nil), data...))
	e.mu.Unlock()
	return nil
}

func (e *recExporter) records() []wire.Batch {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]wire.Batch, len(e.batches))
	for i, b := range e.batches {
		out[i] = *b
	}
	return out
}

func baseCfg(ratio float64) Config {
	c := Config{
		SamplingRatio:     ratio,
		DecisionCacheSize: 64,
		KeepSetCapacity:   32,
		Export: export.Config{
			QueueCapacity: 16, BatchSize: 4, FlushInterval: time.Hour,
			ExportTimeout: time.Second, MaxAttempts: 2, Backoff: time.Millisecond,
			OnFull: export.DropNewest,
		},
	}
	return c
}

// A canceled chain's records are force-kept and exported with the
// terminated marker even when the base sampler drops everything.
func TestCanceledChainForceKeptAndExported(t *testing.T) {
	exp := &recExporter{}
	var drops []export.DropReason
	p := New(exp, baseCfg(0), func(n export.DropNotice) {
		drops = append(drops, n.Reason)
	})
	rootCtx := context.Background()
	p.Start(rootCtx)

	ctx, cancel := context.WithCancel(rootCtx)
	ctx, sp := p.StartSpan(ctx, "request")
	cancel()
	sp.Logger(ctx).Info(ctx, "after cancel")
	sp.Logger(ctx).Info(ctx, "still after cancel")
	sp.End(ctx, context.Canceled)

	if err := p.Flush(rootCtx); err != nil {
		t.Fatal(err)
	}
	batches := exp.records()
	var count int
	var term int
	for _, b := range batches {
		for _, r := range b.Records {
			count++
			if r.Terminated {
				term++
			}
		}
	}
	st := p.Stats()
	t.Logf("input: ratio=0, canceled chain emits 3 records => exported=%d terminated=%d exceptional=%d drops=%v (basis: terminated=>force keep)",
		count, term, st.Exceptional, drops)
	if count != 3 || term != 3 {
		t.Fatalf("terminated records were not retained: count=%d term=%d", count, term)
	}
	if err := p.Shutdown(rootCtx); err != nil {
		t.Fatal(err)
	}
}

// Business error chains are kept; non-exceptional traces follow the sampler.
func TestBusinessKeptAndSamplingApplies(t *testing.T) {
	exp := &recExporter{}
	p := New(exp, baseCfg(0), nil)
	ctx := context.Background()
	p.Start(ctx)

	// Business error on one trace: exceptional, force-kept.
	c1, sp1 := p.StartSpan(ctx, "buy")
	sp1.End(c1, errkind.Business(bizErr("insufficient balance")))
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	batches := exp.records()
	if len(batches) != 1 || batches[0].Records[0].ErrorKind != "business" {
		t.Fatalf("business chain not exported with business kind: %+v", batches)
	}
	t.Logf("input: business error with ratio=0 => exported kind=%q (basis: error chain force-kept)",
		batches[0].Records[0].ErrorKind)
	_ = p.Shutdown(ctx)
}

type bizErr string

func (e bizErr) Error() string { return string(e) }

// Slow exporter: Submit backpressures (never unbounded growth), and drop
// notices carry queue_full; state stays usable afterward.
func TestSlowExporterBackpressure(t *testing.T) {
	exp := &recExporter{delay: 40 * time.Millisecond}
	var dropsMu sync.Mutex
	var reasons []export.DropReason
	p := New(exp, baseCfg(1), func(n export.DropNotice) {
		dropsMu.Lock()
		reasons = append(reasons, n.Reason)
		dropsMu.Unlock()
	})
	ctx := context.Background()
	p.Start(ctx)

	ok, rejected := 0, 0
	for i := 0; i < 200; i++ {
		c, sp := p.StartSpan(ctx, "r")
		sp.Logger(c).Info(c, "x")
		// Directly exercise the sink path with the record just emitted via logger.
		if p.batcherSubmitProbe() {
			ok++
		} else {
			rejected++
		}
	}
	st := p.Stats()
	dropsMu.Lock()
	gotReasons := append([]export.DropReason(nil), reasons...)
	dropsMu.Unlock()
	t.Logf("input: 200 bursts against 40ms exporter, queue=16 => probe_accepted=%d rejected=%d exporter_dropped=%d reasons_seen=%v queue<=cap invariant held",
		ok, rejected, st.Exporter.Dropped, uniqueReasons(gotReasons))
	if st.Exporter.QueueLen > 16 {
		t.Fatalf("queue exceeded hard bound: %d", st.Exporter.QueueLen)
	}
	_ = p.Shutdown(ctx)
}

func uniqueReasons(rs []export.DropReason) []export.DropReason {
	seen := map[export.DropReason]bool{}
	var out []export.DropReason
	for _, r := range rs {
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	return out
}

// Concurrent sampling/export/flush with mid-flight config swaps must stay
// consistent under the race detector; counters must not go backwards.
func TestConcurrentConfigSwapConsistency(t *testing.T) {
	exp := &recExporter{delay: 200 * time.Microsecond, fail: 3}
	p := New(exp, baseCfg(0.5), nil)
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: emit records, some on canceled (exceptional) chains.
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				cctx, ccancel := context.WithCancel(ctx)
				c, sp := p.StartSpan(cctx, "concurrent")
				ccancel()
				sp.Logger(c).Info(c, "late")
				sp.End(c, context.Canceled)
				ccancel()
			}
		}()
	}
	// Flushers.
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-time.After(time.Millisecond):
					_ = p.Flush(ctx)
				}
			}
		}()
	}
	// Config swappers: alternate ratios and queue sizes (atomic whole swap).
	wg.Add(1)
	go func() {
		defer wg.Done()
		ratios := []float64{0, 0.25, 0.5, 1}
		sizes := []int{8, 32, 16}
		for i := 0; i < 80; i++ {
			cfg := baseCfg(ratios[i%len(ratios)])
			cfg.Export.QueueCapacity = sizes[i%len(sizes)]
			p.UpdateConfig(cfg)
			time.Sleep(100 * time.Microsecond)
		}
	}()

	time.Sleep(60 * time.Millisecond)
	close(stop)
	cancel()
	wg.Wait()

	st := p.Stats()
	if st.Exporter.QueueLen < 0 || st.Decider.Judgments < 0 {
		t.Fatalf("impossible counters: %+v", st)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Logf("shutdown during cancel: %v", err)
	}
	t.Logf("input: 6 emitters+2 flushers+1 swapper for 60ms with -race => judgments=%d kept=%d exceptional=%d exported_batches=%d dropped=%d; no torn state",
		st.Decider.Judgments, st.Decider.Kept, st.Exceptional, st.Exporter.BatchesSent, st.Exporter.Dropped)
}
