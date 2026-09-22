package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

func sampleBatch() *Batch {
	return &Batch{
		Version: CurrentVersion,
		BatchID: "batch-1",
		Records: []trace.Record{{
			TraceID:    trace.NewTraceID(),
			SpanID:     trace.NewSpanID(),
			Time:       time.Now().UTC().Truncate(time.Nanosecond),
			Level:      trace.LevelInfo,
			Message:    "hello",
			Fields:     map[string]any{"k": "v", "n": 3.0},
			Terminated: true,
			TermReason: trace.TermCanceled,
			ErrorKind:  "canceled",
		}},
	}
}

func TestRoundTripV1(t *testing.T) {
	b := sampleBatch()
	data, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input batch_id=%s records=%d => encoded: %s", b.BatchID, len(b.Records), data)
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got.Version != Version1 || got.BatchID != b.BatchID {
		t.Fatalf("envelope mismatch: %+v", got)
	}
	g, w := got.Records[0], b.Records[0]
	if g.TraceID != w.TraceID || g.SpanID != w.SpanID || g.Message != w.Message ||
		g.Terminated != w.Terminated || g.TermReason != w.TermReason || g.ErrorKind != w.ErrorKind {
		t.Fatalf("record mismatch:\n got=%+v\nwant=%+v", g, w)
	}
	if g.Fields["k"] != "v" {
		t.Fatalf("fields lost: %+v", g.Fields)
	}
	t.Logf("verdict: V1 round-trip preserves correlation, termination=%v reason=%s fields=%v",
		g.Terminated, g.TermReason, g.Fields)
}

// Simulate a batch written by an OLDER build: a minimal v=1 payload lacking
// newer optional fields must still decode (absent fields take zero values).
func TestOlderPayloadAccepted(t *testing.T) {
	old := []byte(`{"v":1,"batch_id":"old-batch","records":[{"trace_id":"` + trace.NewTraceID().String() + `","span_id":"` + trace.NewSpanID().String() + `","message":"legacy"}]}`)
	t.Logf("input (written by older v1 build, missing time/level/terminated): %s", old)
	b, err := Decode(old)
	if err != nil {
		t.Fatalf("older payload must decode: %v", err)
	}
	r := b.Records[0]
	if r.Message != "legacy" || r.Level != "" || r.Terminated {
		t.Fatalf("legacy decode mismatch: %+v", r)
	}
	t.Logf("verdict: legacy v1 read correctly; absent fields stay zero, message=%q", r.Message)
}

// Unknown fields at envelope and record level must be retained verbatim, not
// dropped or misaligned, and survive re-encoding.
func TestUnknownFieldsPreserved(t *testing.T) {
	payload := []byte(`{
		"v":1,"batch_id":"b1","schema_hint":99,
		"records":[{"trace_id":"` + trace.NewTraceID().String() + `","span_id":"` + trace.NewSpanID().String() + `",
		"message":"m","future_span":{"a":1},"extra":[true,false]}]
	}`)
	t.Logf("input with unknown envelope field schema_hint and record fields future_span/extra: %s", payload)
	b, err := Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(b.Unknown["schema_hint"]) != "99" {
		t.Fatalf("envelope unknown field lost: %+v", b.Unknown)
	}
	r := b.Records[0]
	if _, ok := r.Unknown["future_span"]; !ok {
		t.Fatalf("record unknown field future_span lost: %+v", r.Unknown)
	}
	if _, ok := r.Unknown["extra"]; !ok {
		t.Fatalf("record unknown field extra lost")
	}
	// Re-encode and decode again: unknown data must persist.
	re, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := Decode(re)
	if err != nil {
		t.Fatal(err)
	}
	if string(b2.Unknown["schema_hint"]) != "99" {
		t.Fatalf("unknown envelope field did not survive re-encode: %s", re)
	}
	if _, ok := b2.Records[0].Unknown["future_span"]; !ok {
		t.Fatalf("unknown record field did not survive re-encode: %s", re)
	}
	if !bytes.Contains(re, []byte("future_span")) || !bytes.Contains(re, []byte("schema_hint")) {
		t.Fatalf("re-encoded payload dropped unknown data: %s", re)
	}
	t.Logf("verdict: unknown fields preserved through decode->encode->decode: envelope=%v record=%v",
		[]string{"schema_hint"}, keys(r.Unknown))
}

// Unsupported versions must be explicitly rejected, never read with defaults.
func TestUnsupportedVersionRejected(t *testing.T) {
	cases := map[string][]byte{
		"v0":           []byte(`{"v":0,"batch_id":"x"}`),
		"v2(future)":   []byte(`{"v":2,"batch_id":"x"}`),
		"v999":         []byte(`{"v":999,"batch_id":"x"}`),
		"missing v":    []byte(`{"batch_id":"x"}`),
		"v wrong type": []byte(`{"v":"1"}`),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Decode(data)
			t.Logf("input=%s => err=%v (basis: version outside [%d,%d] -> ErrUnsupportedVersion)",
				data, err, MinSupportedVersion, CurrentVersion)
			if err == nil {
				t.Fatalf("must reject: %s", name)
			}
			if strings.Contains(name, "wrong type") {
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("want ErrMalformed, got %v", err)
				}
				return
			}
			if !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("want ErrUnsupportedVersion, got %v", err)
			}
		})
	}
}

// Malformed payloads must produce ErrMalformed without partially-populated
// results that could be mistaken for valid data.
func TestMalformedRejected(t *testing.T) {
	bad := trace.NewTraceID().String() // valid hex reused for a bad field below
	cases := map[string][]byte{
		"empty":             {},
		"not json":          []byte(`{nope`),
		"bad trace id":      []byte(`{"v":1,"batch_id":"b","records":[{"trace_id":"zz"}]}`),
		"bad time":          []byte(`{"v":1,"batch_id":"b","records":[{"time":"tomorrow"}]}`),
		"records not array": []byte(`{"v":1,"batch_id":"b","records":{"a":1}}`),
		"trace wrong type":  []byte(`{"v":1,"batch_id":"b","records":[{"trace_id":12}]}`),
	}
	_ = bad
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Decode(data)
			t.Logf("input(%s)=%s => err=%v (basis: structural/type/parse failure -> ErrMalformed)",
				name, data, err)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("want ErrMalformed, got %v", err)
			}
		})
	}
}

func TestEncodeNilBatch(t *testing.T) {
	if _, err := Encode(nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("nil batch must be malformed, got %v", err)
	}
}

func TestSupports(t *testing.T) {
	if !Supports(1) || Supports(0) || Supports(2) {
		t.Fatalf("Supports range incorrect")
	}
	var top map[string]json.RawMessage
	_ = json.Unmarshal([]byte(`{"v":1}`), &top)
	t.Logf("supported range [%d,%d] verified", MinSupportedVersion, CurrentVersion)
}

func keys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
