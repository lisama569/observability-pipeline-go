// Demonstrates the in-process observability pipeline: correlated
// trace+logging, error classification, deterministic sampling, bounded
// backpressure and versioned export. Run with:
//
//	go run ./example
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	obs "github.com/lisama569/observability-pipeline-go/internal/observe/errkind"
	"github.com/lisama569/observability-pipeline-go/internal/observe/export"
	"github.com/lisama569/observability-pipeline-go/internal/observe/pipeline"
	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
	"github.com/lisama569/observability-pipeline-go/internal/observe/wire"
)

// stdoutExporter decodes received batches and prints a one-line summary,
// proving the data is versioned and idempotently keyed.
type stdoutExporter struct{}

func (stdoutExporter) Export(ctx context.Context, data []byte) error {
	b, err := wire.Decode(data)
	if err != nil {
		return export.Permanent(err)
	}
	var term, errs int
	for _, r := range b.Records {
		if r.Terminated {
			term++
		}
		if r.ErrorKind != "" {
			errs++
		}
	}
	fmt.Printf("[export] batch=%s v=%d records=%d terminated=%d error_records=%d\n",
		b.BatchID, b.Version, len(b.Records), term, errs)
	return nil
}

func main() {
	cfg := pipeline.Config{
		SamplingRatio:     1,
		DecisionCacheSize: 128,
		KeepSetCapacity:   64,
		Export: export.Config{
			QueueCapacity: 64, BatchSize: 8, FlushInterval: 100 * time.Millisecond,
			ExportTimeout: time.Second, MaxAttempts: 2, Backoff: 5 * time.Millisecond,
			OnFull: export.DropNewest,
		},
	}
	onDrop := func(n export.DropNotice) {
		fmt.Printf("[drop] reason=%s records=%d err=%v\n", n.Reason, len(n.Records), n.Err)
	}
	p := pipeline.New(stdoutExporter{}, cfg, onDrop)
	ctx := context.Background()
	p.Start(ctx)
	defer func() { _ = p.Shutdown(ctx) }()

	// 1) A successful request chain.
	c1, sp1 := p.StartSpan(ctx, "create-order")
	sp1.Logger(c1).Info(c1, "order accepted", trace.F("order", "A1"))
	sp1.End(c1, nil)

	// 2) A business error: classified distinctly, cause preserved.
	c2, sp2 := p.StartSpan(ctx, "charge")
	bizErr := obs.Business(errors.New("card declined"))
	sp2.End(c2, bizErr)
	fmt.Printf("[classify] business outward=%q cause_preserved=%v\n",
		obs.Sanitize(bizErr).Error(), errors.Is(obs.Sanitize(bizErr), bizErr))

	// 3) A canceled chain: records after cancel are marked terminated,
	//    force-retained, and still exported.
	c3, cancel := context.WithCancel(ctx)
	c3, sp3 := p.StartSpan(c3, "slow-report")
	cancel()
	sp3.Logger(c3).Warn(c3, "continuing after cancellation")
	sp3.End(c3, context.Canceled)

	// 4) Detached goroutine: identity propagates by value; parent
	//    termination is observed but does not abort the background work.
	c4, sp4 := p.StartSpan(ctx, "fanout")
	trace.Detach(c4, "worker", func(dc context.Context) {
		sp4.Logger(dc).Info(dc, "work in detached goroutine")
	})
	time.Sleep(50 * time.Millisecond)

	_ = p.Flush(ctx)
	fmt.Printf("[stats] %+v\n", p.Stats())
}
