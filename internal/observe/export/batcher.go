package export

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
	"github.com/lisama569/observability-pipeline-go/internal/observe/wire"
)

// Batcher is a bounded, batching export pipeline. It guarantees:
//
//   - Hard bound on pending data: Submit never blocks; when the queue is
//     full the configured DropPolicy is applied and a drop notice is emitted.
//   - Bounded retries: each batch is attempted at most MaxAttempts times
//     with linear backoff; the same BatchID is used across retries so a
//     deduplicating receiver never exposes duplicates.
//   - Distinguishable discard reasons (queue full / retry exhausted /
//     shutdown / permanent rejection).
//   - Atomic configuration swaps: concurrent sampling/export/flush never
//     observe a half-updated Config.
type Batcher struct {
	exporter Exporter
	cfg      configHolder
	onDrop   func(DropNotice)

	mu     sync.Mutex
	queue  []trace.Record
	closed bool

	flushMu sync.Mutex
	kick    chan struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup
	start  sync.Once

	enqueued    atomic.Int64
	batchesSent atomic.Int64
	retried     atomic.Int64
	dropped     atomic.Int64
	batchSeq    atomic.Uint64
}

// NewBatcher builds a batcher for exporter with cfg and an optional drop
// listener invoked synchronously whenever data is discarded.
func NewBatcher(exporter Exporter, cfg Config, onDrop func(DropNotice)) *Batcher {
	b := &Batcher{
		exporter: exporter,
		kick:     make(chan struct{}, 1),
		onDrop:   onDrop,
	}
	b.cfg.store(cfg)
	return b
}

// Start launches the background flush worker.
func (b *Batcher) Start(ctx context.Context) {
	b.start.Do(func() {
		cctx, cancel := context.WithCancel(ctx)
		b.cancel = cancel
		b.wg.Add(1)
		go b.loop(cctx)
	})
}

func (b *Batcher) signal() {
	select {
	case b.kick <- struct{}{}:
	default:
	}
}

func (b *Batcher) loop(ctx context.Context) {
	defer b.wg.Done()
	for {
		cfg := b.cfg.load()
		timer := time.NewTimer(cfg.FlushInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-b.kick:
		case <-timer.C:
		}
		// Drain in batches; stop if the worker context terminated.
		b.drainAndExport(ctx)
	}
}

// drainAndExport exports every currently queued batch. flushMu serializes it
// against a concurrent manual Flush.
func (b *Batcher) drainAndExport(ctx context.Context) {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()
	for ctx.Err() == nil {
		batch, id := b.takeBatch()
		if len(batch) == 0 {
			return
		}
		if err := b.sendWithRetry(ctx, id, batch); err != nil {
			return
		}
	}
}

// takeBatch removes up to BatchSize records from the front of the queue.
func (b *Batcher) takeBatch() ([]trace.Record, string) {
	cfg := b.cfg.load()
	b.mu.Lock()
	n := cfg.BatchSize
	if n > len(b.queue) {
		n = len(b.queue)
	}
	if n == 0 {
		b.mu.Unlock()
		return nil, ""
	}
	batch := make([]trace.Record, n)
	copy(batch, b.queue[:n])
	// Rebuild the tail so evicted records cannot be retained in backing memory.
	tail := make([]trace.Record, len(b.queue)-n)
	copy(tail, b.queue[n:])
	b.queue = tail
	b.mu.Unlock()
	return batch, b.newBatchID()
}

func (b *Batcher) newBatchID() string {
	var raw [8]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:]) + fmt.Sprintf("-%d", b.batchSeq.Add(1))
}

// Submit enqueues a record without unbounded blocking. It applies the
// configured backpressure/drop policy and reports whether it was accepted.
func (b *Batcher) Submit(rec trace.Record) bool {
	cfg := b.cfg.load()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.drop(ReasonShutdown, []trace.Record{rec}, nil)
		return false
	}
	if len(b.queue) >= cfg.QueueCapacity {
		switch cfg.OnFull {
		case DropOldest:
			dropped := b.queue[0]
			copy(b.queue, b.queue[1:])
			b.queue = b.queue[:len(b.queue)-1]
			b.queue = append(b.queue, rec)
			enqueueSize := len(b.queue)
			b.mu.Unlock()
			b.enqueued.Add(1)
			b.drop(ReasonQueueFull, []trace.Record{dropped}, nil)
			if enqueueSize >= cfg.BatchSize {
				b.signal()
			}
			return true
		default: // DropNewest
			b.mu.Unlock()
			b.drop(ReasonQueueFull, []trace.Record{rec}, nil)
			return false
		}
	}
	b.queue = append(b.queue, rec)
	n := len(b.queue)
	b.mu.Unlock()
	b.enqueued.Add(1)
	if n >= cfg.BatchSize {
		b.signal()
	}
	return true
}

// ExportOnce encodes records as one batch and exports it with bounded retries.
// The supplied batchID is reused on every attempt so retries are idempotent
// at a deduplicating receiver. Failed-undelivered records produce a drop
// notice with a distinguishable reason.
func (b *Batcher) ExportOnce(ctx context.Context, batchID string, records []trace.Record) error {
	if batchID == "" {
		batchID = b.newBatchID()
	}
	return b.sendWithRetry(ctx, batchID, records)
}

func (b *Batcher) sendWithRetry(ctx context.Context, batchID string, records []trace.Record) error {
	cfg := b.cfg.load()
	payload, err := wire.Encode(&wire.Batch{
		Version: wire.CurrentVersion,
		BatchID: batchID,
		Records: records,
	})
	if err != nil {
		b.drop(ReasonPayloadRejected, records, err)
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			lastErr = ctx.Err()
			break
		}
		attemptCtx, cancel := context.WithTimeout(ctx, cfg.ExportTimeout)
		err := b.exporter.Export(attemptCtx, payload)
		cancel()
		if err == nil {
			b.batchesSent.Add(1)
			return nil
		}
		lastErr = err
		if !IsRetryable(err) || attempt == cfg.MaxAttempts {
			break
		}
		b.retried.Add(1)
		// Linear backoff, abortable by cancellation/shutdown.
		wait := cfg.Backoff * time.Duration(attempt)
		select {
		case <-ctx.Done():
			lastErr = ctx.Err()
			attempt = cfg.MaxAttempts // force exit
		case <-time.After(wait):
		}
	}

	reason := reasonFor(lastErr, ctx)
	b.drop(reason, records, lastErr)
	return lastErr
}

func reasonFor(lastErr error, ctx context.Context) DropReason {
	if ctx.Err() != nil {
		return ReasonShutdown
	}
	var ee *ExportError
	if errors.As(lastErr, &ee) && ee.Retryable() {
		return ReasonRetryExhausted
	}
	return ReasonPayloadRejected
}

func (b *Batcher) drop(reason DropReason, records []trace.Record, err error) {
	if n := len(records); n > 0 {
		b.dropped.Add(int64(n))
	}
	if b.onDrop != nil {
		b.onDrop(DropNotice{Reason: reason, Records: records, Err: err})
	}
}

// Flush synchronously exports all currently buffered records.
func (b *Batcher) Flush(ctx context.Context) error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, id := b.takeBatch()
		if len(batch) == 0 {
			return nil
		}
		if err := b.sendWithRetry(ctx, id, batch); err != nil {
			return err
		}
	}
}

// Shutdown stops accepting records, cancels the worker, performs one bounded
// final flush with ctx, then discards anything still pending with the
// shutdown reason. It never blocks past ctx.
func (b *Batcher) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()

	flushErr := b.Flush(ctx)

	b.mu.Lock()
	leftovers := b.queue
	b.queue = nil
	b.mu.Unlock()
	if len(leftovers) > 0 {
		b.drop(ReasonShutdown, leftovers, ctx.Err())
	}
	return flushErr
}

// Stats returns a consistent counters snapshot.
func (b *Batcher) Stats() Stats {
	b.mu.Lock()
	q := len(b.queue)
	b.mu.Unlock()
	return Stats{
		Enqueued:    b.enqueued.Load(),
		BatchesSent: b.batchesSent.Load(),
		Retried:     b.retried.Load(),
		Dropped:     b.dropped.Load(),
		QueueLen:    q,
	}
}

// UpdateConfig atomically swaps the configuration. The normalized full
// struct is installed in one pointer store, so concurrent work never sees a
// half-merged configuration.
func (b *Batcher) UpdateConfig(cfg Config) {
	b.cfg.store(cfg)
	b.signal() // pick up new batch-size/interval promptly
}
