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

// TestRecordUnknownFieldsRoundTrip 记录级未知字段必须在“解码→重编码”的
// 往返中原样保留（不丢、不串位），且批次级未知字段同样保留。
func TestRecordUnknownFieldsRoundTrip(t *testing.T) {
	// 手工构造带记录级未知字段的 v3 字节流，模拟“新版本写出的旧读取端读不懂的数据”。
	recPayload := func() []byte {
		w := &writer{}
		w.u64(5)
		w.u64(6)
		w.u8(byte(LevelWarn))
		w.u8(0)
		w.str("hi")
		w.str("")
		w.u16(0) // no attrs
		// 记录级未知字段区
		w.u32(2)
		w.str("rec-alpha")
		w.bytes([]byte{0xa1, 0xa2})
		w.str("rec-beta")
		w.bytes([]byte{0xb1})
		return w.buf
	}()
	w := &writer{}
	w.buf = append(w.buf, magic...)
	w.u16(Version3)
	w.str("svc")
	w.u32(1)
	w.bytes(recPayload)
	w.u32(1)
	w.str("batch-gamma")
	w.bytes([]byte{0xc1, 0xc2, 0xc3})
	raw := w.buf

	out, err := DecodeBatch(raw)
	if err != nil {
		t.Fatalf("v3 batch with record unknowns must decode: %v", err)
	}
	rec := out.Records[0]
	if !bytes.Equal(rec.Unknown["rec-alpha"], []byte{0xa1, 0xa2}) ||
		!bytes.Equal(rec.Unknown["rec-beta"], []byte{0xb1}) {
		t.Fatalf("记录级未知字段丢失/错位: %v", rec.Unknown)
	}
	if !bytes.Equal(out.Unknown["batch-gamma"], []byte{0xc1, 0xc2, 0xc3}) {
		t.Fatalf("批次级未知字段丢失: %v", out.Unknown)
	}
	if rec.TraceID != 5 || rec.Message != "hi" {
		t.Fatalf("已知字段错位: %+v", rec)
	}

	// 读旧数据再写回：未知字段必须完整保留。
	reencoded, err := EncodeBatch(out)
	if err != nil {
		t.Fatal(err)
	}
	out2, err := DecodeBatch(reencoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out2.Records[0].Unknown["rec-alpha"], []byte{0xa1, 0xa2}) ||
		!bytes.Equal(out2.Records[0].Unknown["rec-beta"], []byte{0xb1}) {
		t.Errorf("重编码后记录级未知字段丢失: %v", out2.Records[0].Unknown)
	}
	if !bytes.Equal(out2.Unknown["batch-gamma"], []byte{0xc1, 0xc2, 0xc3}) {
		t.Errorf("重编码后批次级未知字段丢失: %v", out2.Unknown)
	}
	t.Logf("记录级未知字段往返保留: rec=%v batch=%v",
		out2.Records[0].Unknown, out2.Unknown)
}

// TestReencodeStable 同一份输入反复编码的结果必须字节级稳定一致，便于比对。
func TestReencodeStable(t *testing.T) {
	in := Batch{
		Version: CurrentVersion,
		Source:  "z",
		Records: []Record{
			{
				TraceID: 1, SpanID: 1, Level: LevelInfo, Message: "m",
				Attrs:   map[string]string{"b": "2", "a": "1"},
				Unknown: map[string][]byte{"z": {1}, "a": {2, 3}},
			},
		},
		Unknown: map[string][]byte{"bk": {9}, "ak": {8}},
	}
	first, err := EncodeBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		got, err := EncodeBatch(in)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, first) {
			t.Fatalf("第 %d 次编码结果不稳定（map 迭代序泄漏）", i+1)
		}
	}
	// 解码后重编码也应与原编码一致（规范化往返）。
	decoded, err := DecodeBatch(first)
	if err != nil {
		t.Fatal(err)
	}
	again, err := EncodeBatch(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, first) {
		t.Errorf("解码后重编码与原编码不一致:\n% x\n% x", again, first)
	}
	t.Logf("重编码稳定, %d bytes", len(first))
}

// TestMalformedV3RecordRejected v3 记录帧长度与内容不符（帧内多余/截断字节）
// 必须返回 ErrMalformed，未知字段不得静默错位。
func TestMalformedV3RecordRejected(t *testing.T) {
	// 截断畸形：rlen=2 但帧内无法构成完整字段。
	w := &writer{}
	w.buf = append(w.buf, magic...)
	w.u16(Version3)
	w.str("svc")
	w.u32(1) // 1 record
	w.bytes([]byte{0x01, 0x02})
	w.u32(0) // no batch unknown
	if _, err := DecodeBatch(w.buf); !errors.Is(err, ErrMalformed) {
		t.Errorf("截断的 v3 记录帧必须报 ErrMalformed, got %v", err)
	}

	// 帧内多余字节：合法记录载荷 + 1 个多余字节（nunknown 头缺失必错）。
	payload := encodeRecordV3(Record{TraceID: 1, Message: "ok"})
	payload = append(payload, 0xff)
	w2 := &writer{}
	w2.buf = append(w2.buf, magic...)
	w2.u16(Version3)
	w2.str("svc")
	w2.u32(1)
	w2.bytes(payload)
	w2.u32(0)
	if _, err := DecodeBatch(w2.buf); !errors.Is(err, ErrMalformed) {
		t.Errorf("帧内多余字节必须报 ErrMalformed, got %v", err)
	}
	t.Log("v3 畸形记录帧均被拒绝")
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
		"empty":     {},
		"bad_magic": append([]byte("XX"), good[2:]...),
		"truncated": good[:len(good)/2],
		"trailing":  append(append([]byte{}, good...), 0x00),
		"huge_len":  append([]byte{'O', 'B', 2, 0, 0xff, 0xff, 0xff, 0xff}, 0),
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
