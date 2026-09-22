// Package wire implements versioned, forward/backward-compatible JSON encoding
// of export batches.
//
// Compatibility rules:
//
//   - Every batch is wrapped in an envelope carrying an explicit "v" version.
//   - Versions <= CurrentVersion and >= MinSupportedVersion are accepted; an
//     unsupported version returns ErrUnsupportedVersion (never silently read
//     with defaults).
//   - On decode, unrecognized fields are retained verbatim (Batch.Unknown and
//     Record.Unknown); re-encoding preserves them, so new producers cannot
//     cause silent column shift or data loss for old readers that round-trip.
//   - Missing required fields or wrong types make the batch ErrMalformed.
package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

// Version is a wire format version.
type Version uint16

const (
	// MinSupportedVersion is the oldest version this build can decode.
	MinSupportedVersion Version = 1
	// Version1 is the current wire format version.
	Version1 Version = 1
	// CurrentVersion is what Encode writes.
	CurrentVersion Version = 1
)

// Batch is a unit of export; BatchID is the receiver-side idempotency key.
type Batch struct {
	Version Version
	BatchID string
	Records []trace.Record
	// Unknown preserves unknown envelope-level fields on decode.
	Unknown map[string]json.RawMessage
}

var (
	// ErrUnsupportedVersion is returned for batches whose version is rejected.
	ErrUnsupportedVersion = errors.New("wire: unsupported batch version")
	// ErrMalformed is returned for batches that cannot be parsed.
	ErrMalformed = errors.New("wire: malformed batch")
)

// envelope is the on-the-wire shape for the current version.
type envelope struct {
	Version Version           `json:"v"`
	BatchID string            `json:"batch_id"`
	Records []json.RawMessage `json:"records"`
}

// Encode serializes b at CurrentVersion. Decoding the result must yield an
// equal batch, including any preserved unknown fields.
func Encode(b *Batch) ([]byte, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: nil batch", ErrMalformed)
	}
	recs := make([]json.RawMessage, 0, len(b.Records))
	for i := range b.Records {
		raw, err := encodeRecord(&b.Records[i])
		if err != nil {
			return nil, err
		}
		recs = append(recs, raw)
	}
	// Ordered map for a stable layout; unknown fields are re-emitted.
	root := orderedObject{}
	root.set("v", json.RawMessage(fmt.Appendf(nil, "%d", CurrentVersion)))
	root.set("batch_id", json.RawMessage(mustJSON(b.BatchID)))
	root.set("records", rawArray(recs))
	for k, v := range b.Unknown {
		if k == "v" || k == "batch_id" || k == "records" {
			continue
		}
		root.set(k, v)
	}
	return root.marshal()
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}

func rawArray(items []json.RawMessage) json.RawMessage {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, it := range items {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(it)
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

// knownRecordFields are understood by this version; everything else is unknown.
var knownRecordFields = map[string]struct{}{
	"trace_id": {}, "span_id": {}, "time": {}, "level": {},
	"message": {}, "fields": {}, "terminated": {},
	"term_reason": {}, "error_kind": {},
}

func encodeRecord(r *trace.Record) (json.RawMessage, error) {
	type recordShape struct {
		TraceID    string         `json:"trace_id"`
		SpanID     string         `json:"span_id"`
		Time       string         `json:"time"`
		Level      trace.Level    `json:"level"`
		Message    string         `json:"message"`
		Fields     map[string]any `json:"fields,omitempty"`
		Terminated bool           `json:"terminated"`
		TermReason string         `json:"term_reason,omitempty"`
		ErrorKind  string         `json:"error_kind,omitempty"`
	}
	shape := recordShape{
		TraceID:    r.TraceID.String(),
		SpanID:     r.SpanID.String(),
		Time:       r.Time.UTC().Format(time.RFC3339Nano),
		Level:      r.Level,
		Message:    r.Message,
		Fields:     r.Fields,
		Terminated: r.Terminated,
		TermReason: string(r.TermReason),
		ErrorKind:  r.ErrorKind,
	}
	known, err := json.Marshal(shape)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	obj := orderedObject{}
	if err := json.Unmarshal(known, &obj.m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	for k, v := range r.Unknown {
		if _, ok := knownRecordFields[k]; ok {
			continue
		}
		obj.set(k, v)
	}
	return obj.marshal()
}

// Decode parses a versioned batch. It rejects unsupported versions outright
// and preserves unknown fields instead of silently dropping or misaligning.
func Decode(data []byte) (*Batch, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrMalformed)
	}
	// Stage 1: generic object so we control versioning and unknown handling.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, fmt.Errorf("%w: invalid json: %v", ErrMalformed, err)
	}
	verRaw, ok := top["v"]
	if !ok {
		return nil, fmt.Errorf("%w: missing version field \"v\"", ErrUnsupportedVersion)
	}
	var ver Version
	if err := json.Unmarshal(verRaw, &ver); err != nil {
		return nil, fmt.Errorf("%w: invalid version: %v", ErrMalformed, err)
	}
	if !Supports(ver) {
		return nil, fmt.Errorf("%w: v=%d (supported: %d..%d)",
			ErrUnsupportedVersion, ver, MinSupportedVersion, CurrentVersion)
	}
	b := &Batch{Version: ver, Unknown: map[string]json.RawMessage{}}

	if raw, ok := top["batch_id"]; ok {
		if err := json.Unmarshal(raw, &b.BatchID); err != nil {
			return nil, fmt.Errorf("%w: batch_id: %v", ErrMalformed, err)
		}
	}
	if raw, ok := top["records"]; ok {
		var raws []json.RawMessage
		if err := json.Unmarshal(raw, &raws); err != nil {
			return nil, fmt.Errorf("%w: records: %v", ErrMalformed, err)
		}
		b.Records = make([]trace.Record, 0, len(raws))
		for i, rr := range raws {
			rec, err := decodeRecord(rr)
			if err != nil {
				return nil, fmt.Errorf("%w: record[%d]: %v", ErrMalformed, i, err)
			}
			b.Records = append(b.Records, rec)
		}
	}
	for k, v := range top {
		if k == "v" || k == "batch_id" || k == "records" {
			continue
		}
		b.Unknown[k] = v
	}
	return b, nil
}

func decodeRecord(raw json.RawMessage) (trace.Record, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return trace.Record{}, fmt.Errorf("invalid json: %v", err)
	}
	var rec trace.Record
	if v, ok := m["trace_id"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return rec, fmt.Errorf("trace_id: %v", err)
		}
		tid, err := trace.ParseTraceID(s)
		if err != nil {
			return rec, err
		}
		rec.TraceID = tid
	}
	if v, ok := m["span_id"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return rec, fmt.Errorf("span_id: %v", err)
		}
		sid, err := trace.ParseSpanID(s)
		if err != nil {
			return rec, err
		}
		rec.SpanID = sid
	}
	if v, ok := m["time"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return rec, fmt.Errorf("time: %v", err)
		}
		tm, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return rec, fmt.Errorf("time: %v", err)
		}
		rec.Time = tm
	}
	if v, ok := m["level"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return rec, fmt.Errorf("level: %v", err)
		}
		rec.Level = trace.Level(s)
	}
	if v, ok := m["message"]; ok {
		if err := json.Unmarshal(v, &rec.Message); err != nil {
			return rec, fmt.Errorf("message: %v", err)
		}
	}
	if v, ok := m["fields"]; ok {
		var f map[string]any
		if err := json.Unmarshal(v, &f); err != nil {
			return rec, fmt.Errorf("fields: %v", err)
		}
		rec.Fields = f
	}
	if v, ok := m["terminated"]; ok {
		if err := json.Unmarshal(v, &rec.Terminated); err != nil {
			return rec, fmt.Errorf("terminated: %v", err)
		}
	}
	if v, ok := m["term_reason"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return rec, fmt.Errorf("term_reason: %v", err)
		}
		rec.TermReason = trace.TerminateReason(s)
	}
	if v, ok := m["error_kind"]; ok {
		if err := json.Unmarshal(v, &rec.ErrorKind); err != nil {
			return rec, fmt.Errorf("error_kind: %v", err)
		}
	}
	for k, v := range m {
		if _, known := knownRecordFields[k]; known {
			continue
		}
		if rec.Unknown == nil {
			rec.Unknown = map[string]json.RawMessage{}
		}
		rec.Unknown[k] = v
	}
	return rec, nil
}

// Supports reports whether v can be decoded by this build.
func Supports(v Version) bool {
	return v >= MinSupportedVersion && v <= CurrentVersion
}
