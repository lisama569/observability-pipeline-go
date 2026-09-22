// Package pipeline assembles correlation, error classification, deterministic
// sampling and the bounded export pipeline into one in-process collector.
//
// A record produced on a canceled or timed-out chain is explicitly marked
// terminated and force-retained (an exceptional chain); it is never silently
// dropped. Business/internal/error records are likewise retained as
// exceptional. Ordinary records follow the deterministic per-trace sampler,
// so all records of the same trace share one reproducible verdict. Records
// that lose the sampling judgment are counted (sampled-out) rather than
// entering the bounded export queue, which therefore only ever holds data
// intended for export.
package pipeline

import (
	"context"
	"sync/atomic"

	"github.com/lisama569/observability-pipeline-go/internal/observe/export"
	"github.com/lisama569/observability-pipeline-go/internal/observe/sampling"
	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

// Config is the full collector configuration.
type Config struct {
	// SamplingRatio in [0,1] governs ordinary, non-exceptional records.
	SamplingRatio float64
	// DecisionCacheSize bounds the per-trace decision cache.
	DecisionCacheSize int
	// KeepSetCapacity bounds the exceptional-chain keep-set.
	KeepSetCapacity int
	// Export configures batching, backpressure and retries.
	Export export.Config
}

func (c Config) normalized() Config {
	if c.DecisionCacheSize < 1 {
		c.DecisionCacheSize = 4096
	}
	if c.KeepSetCapacity < 1 {
		c.KeepSetCapacity = 1024
	}
	return c
}

// Stats is a consistent collector-wide snapshot.
type Stats struct {
	Decider     sampling.DeciderStats
	Exporter    export.Stats
	SampledOut  int64
	Exceptional int64
}

// Pipeline is the assembled collector.
type Pipeline struct {
	batcher *export.Batcher
	decider *sampling.Decider

	cfg atomic.Pointer[Config]

	sampledOut  atomic.Int64
	exceptional atomic.Int64
}

// New builds a pipeline that exports through exporter. The exporter is
// wrapped in a deduplicating layer keyed by stable batch ids so retries are
// not visibly duplicated at the receiver.
func New(exporter export.Exporter, cfg Config, onDrop func(export.DropNotice)) *Pipeline {
	cfg = cfg.normalized()
	keep := sampling.NewKeepSet(cfg.KeepSetCapacity)
	decider := sampling.NewDecider(sampling.NewRatioSampler(cfg.SamplingRatio), cfg.DecisionCacheSize, keep)
	dedup := export.NewDedupExporter(exporter, 4096)
	batcher := export.NewBatcher(dedup, cfg.Export, onDrop)
	p := &Pipeline{batcher: batcher, decider: decider}
	p.cfg.Store(&cfg)
	return p
}

// Start launches background processing.
func (p *Pipeline) Start(ctx context.Context) { p.batcher.Start(ctx) }

// Shutdown flushes and releases resources (see export.Batcher.Shutdown).
func (p *Pipeline) Shutdown(ctx context.Context) error { return p.batcher.Shutdown(ctx) }

// StartSpan begins a correlated span. The span emits its final record through
// this pipeline's sink when End is called.
func (p *Pipeline) StartSpan(ctx context.Context, name string) (context.Context, *trace.Span) {
	ctx = trace.WithSink(ctx, p.Sink())
	return trace.StartSpan(ctx, name)
}

// exceptional reports whether a record must be retained regardless of the
// sampling ratio: terminated chains and any classified error chain.
func exceptional(r trace.Record) bool {
	if r.Terminated {
		return true
	}
	return r.ErrorKind != "" && r.ErrorKind != "none"
}

// Sink returns a trace.Sink that classifies, samples and enqueues records.
// Terminated records are marked by the trace layer and are retained here;
// they are never silently dropped.
func (p *Pipeline) Sink() trace.Sink {
	return func(ctx context.Context, r trace.Record) {
		if exceptional(r) {
			p.exceptional.Add(1)
			p.decider.NoteExceptional(r.TraceID)
		}
		decision := p.decider.Decide(ctx, r.TraceID)
		if !decision.Kept() {
			p.sampledOut.Add(1)
			return
		}
		p.batcher.Submit(r)
	}
}

// Stats returns a consistent counters snapshot.
func (p *Pipeline) Stats() Stats {
	return Stats{
		Decider:     p.decider.Stats(),
		Exporter:    p.batcher.Stats(),
		SampledOut:  p.sampledOut.Load(),
		Exceptional: p.exceptional.Load(),
	}
}

// UpdateConfig atomically swaps tunables. The whole Config is installed in a
// single pointer store and the batcher receives its own atomic swap, so
// concurrent sampling/export/flush never observe a half-updated set.
func (p *Pipeline) UpdateConfig(cfg Config) {
	cfg = cfg.normalized()
	p.cfg.Store(&cfg)
	p.decider.UpdateSampler(sampling.NewRatioSampler(cfg.SamplingRatio))
	p.batcher.UpdateConfig(cfg.Export)
}

// Flush forces an export flush.
func (p *Pipeline) Flush(ctx context.Context) error { return p.batcher.Flush(ctx) }
