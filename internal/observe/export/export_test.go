package export

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

// mockExporter is a controllable backend: it can fail transiently N times,
// reject permanently, block slowly, and records the batch ids it accepted.
type mockExporter struct {
	mu          sync.Mutex
	calls       int
	acceptedIDs []string
	payloads    [][]byte
	failRetry   int // number of initial retryable failures
	failPerm    bool
	delay       time.Duration
}

func (m *mockExporter) Export(ctx context.Context, data []byte) error {
	m.mu.Lock()
	m.calls++
	call := m.calls
	delay := m.delay
	failPerm := m.failPerm
	retryLeft := m.failRetry
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	if failPerm {
		return Permanent(errors.New("400 bad request"))
	}
	if retryLeft > 0 {
		m.mu.Lock()
		m.failRetry--
		m.mu.Unlock()
		_ = call
		return Retryable(errors.New("503 temporary"))
	}
	b, err := decodeBatchID(data)
	if err != nil {
		return Permanent(err)
	}
	m.mu.Lock()
	m.acceptedIDs = append(m.acceptedIDs, b)
	m.payloads = append(m.payloads, append([]byte(nil), data...))
	m.mu.Unlock()
	return nil
}

func (m *mockExporter) snapshot() (int, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := append([]string(nil), m.acceptedIDs...)
	return m.calls, ids
}

func rec() trace.Record {
	return trace.Record{TraceID: trace.NewTraceID(), SpanID: trace.NewSpanID(), Level: trace.LevelInfo, Message: "m"}
}

func testCfg() Config {
	return Config{
		QueueCapacity: 4, BatchSize: 2, FlushInterval: time.Hour,
		ExportTimeout: time.Second, MaxAttempts: 3, Backoff: time.Millisecond,
		OnFull: DropNewest,
	}
}

// A slow exporter must not make the collector grow without bound: Submit
// returns false and a queue_full notice is produced under DropNewest.
func TestBackpressureBounded(t *testing.T) {
	mx := &mockExporter{delay: 50 * time.Millisecond}
	var drops []DropNotice
	b := NewBatcher(mx, testCfg(), func(n DropNotice) {
		drops = append(drops, n)
	})
	ctx := context.Background()
	b.Start(ctx)

	accepted := 0
	rejected := 0
	for i := 0; i < 50; i++ {
		if b.Submit(rec()) {
			accepted++
		} else {
			rejected++
		}
	}
	st := b.Stats()
	t.Logf("input: 50 Submit, queue_cap=%d batch=%d slow exporter => accepted=%d rejected=%d dropped_counter=%d (basis: hard queue bound)",
		4, 2, accepted, rejected, st.Dropped)
	if st.QueueLen > 4 {
		t.Fatalf("queue exceeded capacity: %d", st.QueueLen)
	}
	found := false
	for _, d := range drops {
		if d.Reason == ReasonQueueFull {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a queue_full drop notice, got %+v", drops)
	}
	if err := b.Shutdown(context.Background()); err != nil {
		t.Logf("shutdown err (expected under slow exporter): %v", err)
	}
}

func TestDropOldestAdmitsNewest(t *testing.T) {
	mx := &mockExporter{}
	cfg := testCfg()
	cfg.OnFull = DropOldest
	var drops []DropReason
	b := NewBatcher(mx, cfg, func(n DropNotice) { drops = append(drops, n.Reason) })
	for i := 0; i < 6; i++ {
		if !b.Submit(rec()) {
			t.Fatal("DropOldest must always admit the incoming record")
		}
	}
	st := b.Stats()
	t.Logf("input: 6 Submit into cap=4 DropOldest => queue=%d dropped=%d reasons=%v (basis: evict front, append back)",
		st.QueueLen, st.Dropped, drops)
	if st.QueueLen != 4 || st.Dropped != 2 {
		t.Fatalf("unexpected state: len=%d dropped=%d", st.QueueLen, st.Dropped)
	}
}

// Transient failures are retried with the SAME batch id; on success the
// receiver sees the batch exactly once.
func TestRetrySameBatchIDNoDuplicate(t *testing.T) {
	mx := &mockExporter{failRetry: 2}
	var drops []DropNotice
	b := NewBatcher(mx, testCfg(), func(n DropNotice) { drops = append(drops, n) })
	records := []trace.Record{rec(), rec()}
	err := b.ExportOnce(context.Background(), "stable-id", records)
	calls, ids := mx.snapshot()
	t.Logf("input: backend fails twice then ok; batch_id=stable-id => backend_calls=%d accepted_ids=%v err=%v",
		calls, ids, err)
	if err != nil {
		t.Fatalf("eventual success expected: %v", err)
	}
	if calls != 3 {
		t.Fatalf("want 3 attempts, got %d", calls)
	}
	if len(ids) != 1 || ids[0] != "stable-id" {
		t.Fatalf("receiver saw duplicate/different batch: %v", ids)
	}
	if b.Stats().Retried != 2 || len(drops) != 0 {
		t.Fatalf("retry/drop accounting wrong: %+v drops=%+v", b.Stats(), drops)
	}
}

// Exhausted retries discard with the distinguishable retry_exhausted reason;
// the backend must never have accepted the data.
func TestRetryExhaustedDropsDistinguishably(t *testing.T) {
	mx := &mockExporter{failRetry: 99}
	var drops []DropNotice
	b := NewBatcher(mx, testCfg(), func(n DropNotice) { drops = append(drops, n) })
	err := b.ExportOnce(context.Background(), "lost-id", []trace.Record{rec()})
	calls, ids := mx.snapshot()
	t.Logf("input: backend always 503 max_attempts=3 => calls=%d accepted=%v err_nonnil=%v drops=%d reason=%s (basis: bounded retry then drop with reason)",
		calls, ids, err != nil, len(drops), drops[0].Reason)
	if calls != 3 {
		t.Fatalf("want exactly 3 attempts, got %d", calls)
	}
	if len(ids) != 0 {
		t.Fatalf("nothing must be accepted: %v", ids)
	}
	if len(drops) != 1 || drops[0].Reason != ReasonRetryExhausted {
		t.Fatalf("want one retry_exhausted drop, got %+v", reasons(drops))
	}
}

// Permanent failures are not retried and are dropped as payload_rejected.
func TestPermanentRejectedNoRetry(t *testing.T) {
	mx := &mockExporter{failPerm: true}
	var drops []DropNotice
	b := NewBatcher(mx, testCfg(), func(n DropNotice) { drops = append(drops, n) })
	_ = b.ExportOnce(context.Background(), "p", []trace.Record{rec()})
	calls, _ := mx.snapshot()
	t.Logf("input: backend 400 permanent => calls=%d drop_reason=%s (basis: permanent errors attempt once)",
		calls, drops[0].Reason)
	if calls != 1 || drops[0].Reason != ReasonPayloadRejected {
		t.Fatalf("want single attempt + payload_rejected, got calls=%d drops=%v", calls, reasons(drops))
	}
}

// The DedupExporter must suppress redelivery of an already accepted batch id.
func TestDedupSuppressesDuplicate(t *testing.T) {
	mx := &mockExporter{}
	d := NewDedupExporter(mx, 8)
	data, err := encodeBatch("dup-id", rec())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := d.Export(context.Background(), data); err != nil {
			t.Fatal(err)
		}
	}
	calls, ids := mx.snapshot()
	t.Logf("input: same payload (batch_id=dup-id) exported 3 times => inner_backend_calls=%d accepted=%v seen_cache=%d",
		calls, ids, d.SeenCount())
	if calls != 1 || len(ids) != 1 {
		t.Fatalf("receiver observed duplicates: calls=%d ids=%v", calls, ids)
	}
}

func TestDedupMalformedPermanent(t *testing.T) {
	d := NewDedupExporter(&mockExporter{}, 4)
	err := d.Export(context.Background(), []byte(`{"v":99}`))
	t.Logf("input: unsupported-version payload through dedup => err=%v retryable=%v", err, IsRetryable(err))
	if err == nil || IsRetryable(err) {
		t.Fatalf("malformed payload must be a permanent rejection")
	}
}

// Submitting after shutdown yields a shutdown drop, not a hang.
func TestSubmitAfterShutdownDrops(t *testing.T) {
	mx := &mockExporter{}
	got := make(chan DropReason, 1)
	b := NewBatcher(mx, testCfg(), func(n DropNotice) { got <- n.Reason })
	b.Start(context.Background())
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	ok := b.Submit(rec())
	t.Logf("input: Submit after Shutdown => accepted=%v drop_reason=%s (basis: closed queue)", ok, <-got)
	if ok {
		t.Fatal("post-shutdown submit must be rejected")
	}
}

func TestStatsConsistency(t *testing.T) {
	mx := &mockExporter{}
	b := NewBatcher(mx, testCfg(), nil)
	for i := 0; i < 3; i++ {
		b.Submit(rec())
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := b.Stats()
	t.Logf("input: 3 submits then Flush => enqueued=%d batches_sent=%d queue=%d",
		st.Enqueued, st.BatchesSent, st.QueueLen)
	if st.Enqueued != 3 || st.BatchesSent != 2 || st.QueueLen != 0 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func reasons(ns []DropNotice) []DropReason {
	out := make([]DropReason, len(ns))
	for i, n := range ns {
		out[i] = n.Reason
	}
	return out
}
