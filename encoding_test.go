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

// sampleV3Batch 同时带批次级与记录级未知字段。
func sampleV3Batch() Batch {
	return Batch{
		ID:     11,
		Source: "billing",
		Records: []Record{
			{
				TraceID: 1, SpanID: 1, Level: LevelInfo, Message: "a",
				Attrs: map[string]string{"z": "1", "a": "2"},
				// 注意故意乱序插入，验证按 tag 排序写出。
				Unknown: map[uint16][]byte{300: {0xcc}, 7: {1, 2, 3, 4}, 42: {}},
			},
			{TraceID: 2, SpanID: 2, Level: LevelWarn, Message: "b"},
		},
		Unknown: map[string][]byte{"batch-extra": {0xde, 0xad}},
	}
}

// TestRecordUnknownRoundTrip：记录级与批次级未知字段都必须在
// “解码 → 重新编码”往返中完整保住，字节级一致、不丢不错位。
func TestRecordUnknownRoundTrip(t *testing.T) {
	in := sampleV3Batch()
	data, err := EncodeBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if out.Version != Version3 {
		t.Fatalf("写出应为 v3, got %d", out.Version)
	}
	u := out.Records[0].Unknown
	if len(u) != 3 {
		t.Fatalf("记录级未知字段数量=%d, want 3: %v", len(u), u)
	}
	for tag, want := range map[uint16][]byte{300: {0xcc}, 7: {1, 2, 3, 4}, 42: {}} {
		if !bytes.Equal(u[tag], want) {
			t.Errorf("tag %d 值丢失或错位: got %v want %v", tag, u[tag], want)
		}
	}
	if out.Records[1].Unknown != nil {
		t.Errorf("没有未知字段的记录应保持 nil, got %v", out.Records[1].Unknown)
	}
	if !bytes.Equal(out.Unknown["batch-extra"], []byte{0xde, 0xad}) {
		t.Errorf("批次级未知字段丢失: %v", out.Unknown)
	}
	if out.Records[0].Attrs["z"] != "1" || out.Records[0].Attrs["a"] != "2" {
		t.Errorf("已知字段未正常解析: %v", out.Records[0].Attrs)
	}
}

// TestReencodeStable：同一份输入反复编码，结果必须字节级稳定一致；
// 解码后重新编码的结果也必须与首次编码一致（往返无损 + 确定性）。
func TestReencodeStable(t *testing.T) {
	in := sampleV3Batch()
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
			t.Fatalf("第 %d 次编码与首次不一致", i+1)
		}
	}
	decoded, err := DecodeBatch(first)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodeBatch(decoded)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("首次编码 %d 字节, 往返后重编码 %d 字节", len(first), len(reencoded))
	if !bytes.Equal(reencoded, first) {
		t.Fatalf("旧数据读入后重编码与原字节不一致：记录级未知字段未被无损保留")
	}
}

// encodeV3WithFutureRecordFields 模拟“未来版本写出器”：
// 在 v3 记录的已知字段后追加当前版本不认识的记录级字段（高标签号），
// 并在批次尾部加批次级未来字段。当前版本读取时必须原样保留。
func encodeV3WithFutureRecordFields() []byte {
	w := &writer{}
	w.buf = append(w.buf, magic...)
	w.u16(Version3)
	w.str("svc")
	w.u32(1) // 1 record
	// 已知部分
	w.u64(555)
	w.u64(1)
	w.u8(byte(LevelInfo))
	w.u8(0)
	w.str("msg")
	w.str("")
	w.u16(0) // no attrs
	// 未来记录字段（2 个，按 tag 排序写出）
	w.u16(2)
	w.u16(9001)
	w.bytes([]byte{0x01, 0x02, 0x03})
	w.u16(9002)
	w.bytes([]byte("future-record-value"))
	// 未来批次字段
	w.u32(1)
	w.str("future-batch-key")
	w.bytes([]byte{0x99})
	return w.buf
}

// TestFutureRecordFieldsPreserved：前向兼容——当前版本读到未来版本
// 写入的记录级/批次级字段，必须原样保留并重编码回去。
func TestFutureRecordFieldsPreserved(t *testing.T) {
	data := encodeV3WithFutureRecordFields()
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("带未来记录字段的批次必须可读: %v", err)
	}
	if !bytes.Equal(out.Records[0].Unknown[9001], []byte{1, 2, 3}) {
		t.Errorf("未来记录字段 9001 丢失: %v", out.Records[0].Unknown)
	}
	if string(out.Records[0].Unknown[9002]) != "future-record-value" {
		t.Errorf("未来记录字段 9002 丢失: %v", out.Records[0].Unknown)
	}
	if !bytes.Equal(out.Unknown["future-batch-key"], []byte{0x99}) {
		t.Errorf("未来批次字段丢失: %v", out.Unknown)
	}
	// 已知字段仍正常
	if out.Records[0].TraceID != 555 || out.Records[0].Message != "msg" {
		t.Errorf("已知字段错位: %+v", out.Records[0])
	}
	// 重新编码后未来字段仍在，且再次解码仍一致（多轮往返稳定）。
	once, _ := EncodeBatch(out)
	out2, err := DecodeBatch(once)
	if err != nil {
		t.Fatal(err)
	}
	twice, _ := EncodeBatch(out2)
	if !bytes.Equal(once, twice) {
		t.Error("带未来字段的批次重编码不稳定")
	}
	if !bytes.Equal(out2.Records[0].Unknown[9001], []byte{1, 2, 3}) {
		t.Error("二次往返后未来记录字段丢失")
	}
}

// TestV1V2ReadReencodeToV3：旧版本批次读取后按当前版本重编码，
// 内容完整，记录级未知字段区为空但结构合法，可再次读回。
func TestV1V2ReadReencodeToV3(t *testing.T) {
	recs := []Record{{TraceID: 9, SpanID: 1, Level: LevelWarn, Message: "legacy"}}
	data := encodeV1(recs, map[string][]byte{"old-extra": {1, 2, 3}})
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodeBatch(out)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeBatch(reencoded)
	if err != nil {
		t.Fatalf("重编码批次必须可读: %v", err)
	}
	if back.Version != Version3 {
		t.Errorf("重编码应为当前版本 v3, got %d", back.Version)
	}
	if back.Records[0].Message != "legacy" {
		t.Errorf("旧数据内容丢失: %+v", back.Records[0])
	}
	if !bytes.Equal(back.Unknown["old-extra"], []byte{1, 2, 3}) {
		t.Errorf("批次级未知字段在版本升级后丢失")
	}
}

// TestDuplicateRecordTagRejected：记录级未知字段出现重复标签时必须判畸形，
// 不允许静默覆盖造成语义歧义。
func TestDuplicateRecordTagRejected(t *testing.T) {
	w := &writer{}
	w.buf = append(w.buf, magic...)
	w.u16(Version3)
	w.str("")
	w.u32(1)
	w.u64(1)
	w.u64(1)
	w.u8(0)
	w.u8(0)
	w.str("m")
	w.str("")
	w.u16(0)
	w.u16(2)
	w.u16(7)
	w.bytes([]byte{1})
	w.u16(7) // duplicate tag
	w.bytes([]byte{2})
	w.u32(0)
	if _, err := DecodeBatch(w.buf); !errors.Is(err, ErrMalformed) {
		t.Fatalf("重复记录标签应判畸形, got %v", err)
	}
}
