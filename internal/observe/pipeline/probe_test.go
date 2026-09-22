package pipeline

import "github.com/lisama569/observability-pipeline-go/internal/observe/trace"

// probeEnqueue submits a benign record straight to the bounded batcher,
// letting tests assert backpressure independently of sampler outcomes.
func (p *Pipeline) probeEnqueue() bool {
	return p.batcher.Submit(trace.Record{
		TraceID: trace.NewTraceID(),
		SpanID:  trace.NewSpanID(),
		Level:   trace.LevelDebug,
		Message: "backpressure-probe",
	})
}

func (p *Pipeline) batcherSubmitProbe() bool { return p.probeEnqueue() }
