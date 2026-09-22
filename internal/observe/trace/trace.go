// Package trace provides trace/span correlation, structured logging
// records and cross-goroutine propagation of correlation information.
//
// Correlation (trace id, span id, parent id) is carried by value in a
// context, so dispatching work to another goroutine copies the identity and
// cannot cross-talk with siblings. After a context is canceled or its
// deadline expires, records produced on that chain are explicitly marked
// (Record.Terminated/TermReason) instead of being silently dropped.
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/observability-pipeline-go/internal/observe/errkind"
)

// TraceID identifies a complete request chain.
type TraceID [16]byte

// SpanID identifies a single span.
type SpanID [8]byte

// TerminateReason marks records produced after a chain was terminated.
type TerminateReason string

const (
	TermNone     TerminateReason = ""
	TermCanceled TerminateReason = "canceled"
	TermTimeout  TerminateReason = "deadline_exceeded"
)

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string  { return hex.EncodeToString(s[:]) }

// ParseTraceID parses a 32-char hex trace id.
func ParseTraceID(s string) (TraceID, error) {
	var t TraceID
	b, err := hex.DecodeString(s)
	if err != nil {
		return t, fmt.Errorf("parse trace id: %w", err)
	}
	if len(b) != len(t) {
		return t, fmt.Errorf("parse trace id: expected %d bytes, got %d", len(t), len(b))
	}
	copy(t[:], b)
	return t, nil
}

// ParseSpanID parses a 16-char hex span id.
func ParseSpanID(s string) (SpanID, error) {
	var sp SpanID
	b, err := hex.DecodeString(s)
	if err != nil {
		return sp, fmt.Errorf("parse span id: %w", err)
	}
	if len(b) != len(sp) {
		return sp, fmt.Errorf("parse span id: expected %d bytes, got %d", len(sp), len(b))
	}
	copy(sp[:], b)
	return sp, nil
}

// NewTraceID returns a cryptographically random trace id.
func NewTraceID() TraceID {
	var t TraceID
	_, _ = rand.Read(t[:])
	return t
}

// NewSpanID returns a cryptographically random span id.
func NewSpanID() SpanID {
	var s SpanID
	_, _ = rand.Read(s[:])
	return s
}

// Correlation is the per-chain identity attached to every record.
type Correlation struct {
	TraceID    TraceID
	SpanID     SpanID
	ParentID   SpanID
	Terminated bool
	TermReason TerminateReason
}

// Level is a log severity.
type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// Field is a structured log field.
type Field struct {
	Key   string
	Value any
}

// F is a shorthand for constructing a field.
func F(key string, value any) Field { return Field{Key: key, Value: value} }

// Record is a single correlated log event.
type Record struct {
	TraceID    TraceID         `json:"trace_id"`
	SpanID     SpanID          `json:"span_id"`
	Time       time.Time       `json:"time"`
	Level      Level           `json:"level"`
	Message    string          `json:"message"`
	Fields     map[string]any  `json:"fields,omitempty"`
	Terminated bool            `json:"terminated"`
	TermReason TerminateReason `json:"term_reason,omitempty"`
	ErrorKind  string          `json:"error_kind,omitempty"`
	// Unknown preserves fields not understood by the wire decoder.
	Unknown map[string]json.RawMessage `json:"-"`
}

// Sink receives emitted records.
type Sink func(context.Context, Record)

type contextKey int

const (
	corrKey contextKey = iota
	sinkKey
	parentCtxKey
)

// WithCorrelation returns a context carrying the correlation.
func WithCorrelation(ctx context.Context, c Correlation) context.Context {
	return context.WithValue(ctx, corrKey, c)
}

// CorrelationFrom extracts the correlation from a context.
func CorrelationFrom(ctx context.Context) (Correlation, bool) {
	c, ok := ctx.Value(corrKey).(Correlation)
	return c, ok
}

// WithSink returns a context carrying the sink.
func WithSink(ctx context.Context, s Sink) context.Context {
	return context.WithValue(ctx, sinkKey, s)
}

func sinkFrom(ctx context.Context) (Sink, bool) {
	s, ok := ctx.Value(sinkKey).(Sink)
	return s, ok
}

// terminationOf derives the termination state at record emission time from
// the context (and, for detached work, the originating parent context).
// An already-terminated correlation takes precedence.
func terminationOf(ctx context.Context, c Correlation) (bool, TerminateReason) {
	if c.Terminated {
		return true, c.TermReason
	}
	err := ctx.Err()
	if err == nil {
		if pc, ok := ctx.Value(parentCtxKey).(context.Context); ok {
			err = pc.Err()
		}
	}
	if err == nil {
		return false, TermNone
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, TermTimeout
	}
	return true, TermCanceled
}

// StartSpan creates a child correlation under the correlation in ctx (or a
// fresh root chain if none exists) and returns the span and a context that
// carries both the child correlation and the sink of ctx.
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	parent, _ := CorrelationFrom(ctx)
	c := Correlation{TraceID: parent.TraceID, ParentID: parent.SpanID, SpanID: NewSpanID()}
	var zero TraceID
	if c.TraceID == zero {
		c.TraceID = NewTraceID()
	}
	// Inherit an already-known termination state.
	if parent.Terminated {
		c.Terminated = true
		c.TermReason = parent.TermReason
	}
	sctx := WithCorrelation(ctx, c)
	sp := &Span{corr: c, start: time.Now().UTC(), name: name}
	return sctx, sp
}

// Logger emits records bound to a context's correlation. It is safe for
// concurrent use.
type Logger struct {
	base context.Context
}

// NewLogger returns a logger. The base context provides a fallback sink
// lookup; the context passed to each log call supplies the correlation and
// live termination state.
func NewLogger(ctx context.Context) *Logger { return &Logger{base: ctx} }

func (l *Logger) resolveSink(ctx context.Context) (Sink, bool) {
	if s, ok := sinkFrom(ctx); ok {
		return s, true
	}
	if l.base != nil {
		return sinkFrom(l.base)
	}
	return nil, false
}

// Log emits a record at the given level. If the chain (or, for detached
// work, its originating chain) is canceled or past its deadline, the record
// is emitted with a termination marker rather than being dropped.
func (l *Logger) Log(ctx context.Context, level Level, msg string, fields ...Field) {
	s, ok := l.resolveSink(ctx)
	if !ok {
		return
	}
	c, _ := CorrelationFrom(ctx)
	if cb, ok2 := CorrelationFrom(l.base); ok2 && c == (Correlation{}) {
		c = cb
	}
	term, reason := terminationOf(ctx, c)
	if term {
		c.Terminated = true
		c.TermReason = reason
	}
	rec := Record{
		TraceID:    c.TraceID,
		SpanID:     c.SpanID,
		Time:       time.Now().UTC(),
		Level:      level,
		Message:    msg,
		Fields:     fieldMap(fields),
		Terminated: term,
		TermReason: reason,
	}
	s(ctx, rec)
}

func fieldMap(fields []Field) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]any, len(fields))
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	return m
}

// Debug emits a debug record.
func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelDebug, msg, fields...)
}

// Info emits an info record.
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelInfo, msg, fields...)
}

// Warn emits a warn record.
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelWarn, msg, fields...)
}

// Error emits an error record. The error is classified: its category is
// recorded and canceled/timed-out errors mark the chain terminated.
func (l *Logger) Error(ctx context.Context, msg string, err error, fields ...Field) {
	class := errkind.Classify(err)
	fs := append([]Field(nil), fields...)
	if err != nil {
		fs = append(fs, F("error", err.Error()), F("error_kind", class.Kind().String()))
	}
	s, ok := l.resolveSink(ctx)
	if !ok {
		return
	}
	c, _ := CorrelationFrom(ctx)
	term, reason := terminationOf(ctx, c)
	if class.Terminated() {
		term = true
		if reason == TermNone {
			if class.Kind() == errkind.KindDeadlineExceeded {
				reason = TermTimeout
			} else {
				reason = TermCanceled
			}
		}
	}
	if term {
		c.Terminated = true
		c.TermReason = reason
	}
	kind := ""
	if err != nil {
		kind = class.Kind().String()
	}
	s(ctx, Record{
		TraceID:    c.TraceID,
		SpanID:     c.SpanID,
		Time:       time.Now().UTC(),
		Level:      LevelError,
		Message:    msg,
		Fields:     fieldMap(fs),
		Terminated: term,
		TermReason: reason,
		ErrorKind:  kind,
	})
}

// Span models an in-progress span.
type Span struct {
	mu    sync.Mutex
	corr  Correlation
	start time.Time
	name  string
	ended bool
}

// Correlation returns the span's correlation.
func (s *Span) Correlation() Correlation { return s.corr }

// Name returns the span name.
func (s *Span) Name() string { return s.name }

// Start returns the span start time.
func (s *Span) Start() time.Time { return s.start }

// Logger returns a logger bound to the span context.
func (s *Span) Logger(ctx context.Context) *Logger {
	// Ensure the span's own correlation is present even when the caller
	// passes a context that does not carry it.
	if _, ok := CorrelationFrom(ctx); !ok {
		ctx = WithCorrelation(ctx, s.corr)
	}
	return NewLogger(ctx)
}

// End closes the span at most once and emits a final record that classifies
// err. A canceled/timed-out context or classified terminal error marks the
// final record as a terminated chain; internal error details are recorded in
// the error category only, not as an outward conclusion.
func (s *Span) End(ctx context.Context, err error) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	c := s.corr
	name := s.name
	s.mu.Unlock()

	l := NewLogger(ctx)
	if _, ok := CorrelationFrom(ctx); !ok {
		ctx = WithCorrelation(ctx, c)
	}
	if err != nil {
		l.Error(ctx, "span.end: "+name, err, F("span", name), F("duration_ms", ms(time.Since(s.start))))
		return
	}
	l.Log(ctx, LevelInfo, "span.end: "+name, F("span", name), F("duration_ms", ms(time.Since(s.start))))
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

// Detach runs fn in a new goroutine with an explicitly propagated copy of
// the current correlation and sink. The goroutine receives a context that is
// never canceled itself (so dispatched work is not aborted by the parent),
// but it retains a reference to the originating context: records emitted
// after the parent is canceled or times out are still marked as belonging to
// a terminated chain. Because correlation propagates by value, concurrent
// siblings cannot cross-talk.
func Detach(ctx context.Context, name string, fn func(ctx context.Context)) {
	c, _ := CorrelationFrom(ctx)
	cp := c // explicit independent copy
	base := context.Background()
	base = WithCorrelation(base, cp)
	if s, ok := sinkFrom(ctx); ok {
		base = WithSink(base, s)
	}
	base = context.WithValue(base, parentCtxKey, ctx)
	go func() {
		fn(base)
	}()
}
