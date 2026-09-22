package export

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

// DropPolicy decides which record is discarded when the bounded queue is full.
type DropPolicy int

const (
	// DropNewest rejects the incoming record (keeps already-queued work).
	DropNewest DropPolicy = iota
	// DropOldest evicts the oldest queued record to admit the new one.
	DropOldest
)

// DropReason distinguishes why data could not be delivered. These are the
// "distinguishable reasons" recorded when data must be discarded.
type DropReason string

const (
	// ReasonQueueFull: the bounded queue was full and backpressure applied.
	ReasonQueueFull DropReason = "queue_full"
	// ReasonRetryExhausted: retries were exhausted for a transient failure.
	ReasonRetryExhausted DropReason = "retry_exhausted"
	// ReasonShutdown: the pipeline shut down before delivery.
	ReasonShutdown DropReason = "shutdown"
	// ReasonPayloadRejected: the backend permanently rejected the batch.
	ReasonPayloadRejected DropReason = "payload_rejected"
)

// Exporter delivers an encoded batch to a backend.
type Exporter interface {
	Export(ctx context.Context, data []byte) error
}

// ExportError classifies an export failure as retryable or permanent.
type ExportError struct {
	retryable bool
	cause     error
}

// Error implements error.
func (e *ExportError) Error() string {
	if e == nil || e.cause == nil {
		return "export error"
	}
	kind := "permanent"
	if e.retryable {
		kind = "retryable"
	}
	return kind + " export error: " + e.cause.Error()
}

// Unwrap returns the underlying cause.
func (e *ExportError) Unwrap() error { return e.cause }

// Retryable reports whether the failure may be retried.
func (e *ExportError) Retryable() bool { return e != nil && e.retryable }

// Retryable wraps err as a transient, retryable export failure.
func Retryable(err error) *ExportError {
	return &ExportError{retryable: true, cause: err}
}

// Permanent wraps err as a non-retryable export failure.
func Permanent(err error) *ExportError {
	return &ExportError{retryable: false, cause: err}
}

// IsRetryable reports whether err should be retried. An ExportError decides
// via Retryable(); context cancellation/deadline are not retried.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var ee *ExportError
	if errors.As(err, &ee) {
		return ee.Retryable()
	}
	// Unknown errors are treated as permanent: no surprise retry storms.
	return false
}

// Config controls batcher capacity, timing and retry behavior.
type Config struct {
	// QueueCapacity bounds the pending-record buffer (hard memory bound).
	QueueCapacity int
	// BatchSize triggers a flush when reached.
	BatchSize int
	// FlushInterval is the maximum time a partial batch waits.
	FlushInterval time.Duration
	// ExportTimeout bounds a single export attempt.
	ExportTimeout time.Duration
	// MaxAttempts bounds retries per batch (>=1).
	MaxAttempts int
	// Backoff is the base delay between retries (linear backoff).
	Backoff time.Duration
	// OnFull selects the queue-full backpressure policy.
	OnFull DropPolicy
}

// DefaultConfig returns a safe bounded configuration.
func DefaultConfig() Config {
	return Config{
		QueueCapacity: 1024,
		BatchSize:     64,
		FlushInterval: time.Second,
		ExportTimeout: 5 * time.Second,
		MaxAttempts:   3,
		Backoff:       50 * time.Millisecond,
		OnFull:        DropNewest,
	}
}

func (c Config) normalized() Config {
	d := DefaultConfig()
	if c.QueueCapacity < 1 {
		c.QueueCapacity = d.QueueCapacity
	}
	if c.BatchSize < 1 {
		c.BatchSize = d.BatchSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = d.FlushInterval
	}
	if c.ExportTimeout <= 0 {
		c.ExportTimeout = d.ExportTimeout
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.Backoff <= 0 {
		c.Backoff = d.Backoff
	}
	return c
}

// DropNotice describes discarded records with a distinguishable reason.
type DropNotice struct {
	Reason  DropReason
	Records []trace.Record
	Err     error
}

// Stats is a consistent snapshot of batcher counters.
type Stats struct {
	Enqueued    int64
	BatchesSent int64
	Retried     int64
	Dropped     int64
	QueueLen    int
}

// configHolder provides atomic, half-update-free config access.
type configHolder struct {
	v atomic.Pointer[Config]
}

func (h *configHolder) load() Config {
	p := h.v.Load()
	if p == nil {
		return DefaultConfig()
	}
	return *p
}

func (h *configHolder) store(c Config) {
	c = c.normalized()
	h.v.Store(&c)
}
