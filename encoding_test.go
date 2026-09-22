package pipeline

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func sampleBatch() Batch {
	return Batch{
		ID:      7,
		Version: CurrentVersion,
		Source:  "checkout-service",
		Records: []Record{
			{TraceID: 42, SpanID: 1, Level: LevelInfo, Message: "start", Attrs: map[string]string{"k": "v"}},
			{TraceID: 42, SpanID: 2, Level: LevelError, Message: "boom", Terminated: true, TermReason: "timeout"},
		},
		Unknown: map[string][]byte{"future-field": {0xde, 0xad}},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := sampleBatch()
	data, err := EncodeBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input=%+v decoded=%+v", in.Records, out.Records)
	if out.Source != in.Source || len(out.Records) != 2 {
		t.Fatalf("round trip mismatch: %+v", out)
	}
	if out.Records[1].TermReason != "timeout" || !out.Records[1].Terminated {
		t.Errorf("terminated record mismatch: %+v", out.Records[1])
	}
	if !bytes.Equal(out.Unknown["future-field"], []byte{0xde, 0xad}) {
		t.Errorf("unknown field lost: %v", out.Unknown)
	}
}

// 构造一个 v1 批次（无 Source 字段），验证新版本能读取旧数据。
func encodeV1(recs []Record, unknown map[string][]byte) []byte {
	w := &writer{}
	w.buf = append(w.buf, magic...)
	w.u16(Version1)
	w.u32(uint32(len(recs)))
	for _, rec := range recs {
		w.u64(rec.TraceID)
		w.u64(rec.SpanID)
		w.u8(byte(rec.Level))
		var term byte
		if rec.Terminated {
			term = 1
		}
		w.u8(term)
		w.str(rec.Message)
		w.str(rec.TermReason)
		w.u16(0) // no attrs
	}
	w.u32(uint32(len(unknown)))
	for k, v := range unknown {
		w.str(k)
		w.bytes(v)
	}
	return w.buf
}

func TestDecodeV1Batch(t *testing.T) {
	recs := []Record{{TraceID: 9, SpanID: 1, Level: LevelWarn, Message: "legacy"}}
	data := encodeV1(recs, map[string][]byte{"old-extra": {1, 2, 3}})
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("v1 batch must be readable: %v", err)
	}
	t.Logf("v1 input records=%+v decoded version=%d source=%q unknown=%v",
		recs, out.Version, out.Source, out.Unknown)
	if out.Version != Version1 {
		t.Errorf("version=%d, want %d", out.Version, Version1)
	}
	if out.Source != "" {
		t.Errorf("v1 has no Source, got %q", out.Source)
	}
	if len(out.Records) != 1 || out.Records[0].Message != "legacy" {
		t.Errorf("records mismatch: %+v", out.Records)
	}
	if !bytes.Equal(out.Unknown["old-extra"], []byte{1, 2, 3}) {
		t.Errorf("v1 unknown field lost")
	}
}

func TestUnsupportedVersionRejected(t *testing.T) {
	data := encodeV1(nil, nil)
	binary.LittleEndian.PutUint16(data[2:4], 99)
	_, err := DecodeBatch(data)
	t.Logf("version=99 判定: err=%v", err)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got %v", err)
	}
}

func TestMalformedBatchRejected(t *testing.T) {
	good, _ := EncodeBatch(sampleBatch())
	cases := map[string][]byte{
		"empty":        {},
		"bad_magic":    append([]byte("XX"), good[2:]...),
		"truncated":    good[:len(good)/2],
		"trailing":     append(append([]byte{}, good...), 0x00),
		"huge_len":     append([]byte{'O', 'B', 2, 0, 0xff, 0xff, 0xff, 0xff}, 0),
	}
	for name, data := range cases {
		_, err := DecodeBatch(data)
		t.Logf("case=%s len=%d 判定: err=%v", name, len(data), err)
		if err == nil {
			t.Errorf("%s: malformed batch must be rejected", name)
		}
		if name == "bad_magic" && !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: want ErrMalformed, got %v", name, err)
		}
	}
}
