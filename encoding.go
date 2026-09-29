package pipeline

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// 批次编码版本。
const (
	// Version1 初始版本：版本头 + 记录区 + 批次级未知字段区。
	Version1 uint16 = 1
	// Version2 在版本头后新增 Source 字段；旧版本写出的批次仍可读。
	Version2 uint16 = 2
	// Version3 在每条记录尾部新增记录级未知字段区，使记录自身新增字段
	// 在“旧版本读取 → 重新编码”的往返中也能被完整保留。
	Version3 uint16 = 3
	// CurrentVersion 是当前写出版本。
	CurrentVersion = Version3
)

var magic = []byte{'O', 'B'}

// ErrUnsupportedVersion 表示批次版本不被支持，必须明确拒绝。
var ErrUnsupportedVersion = errors.New("unsupported batch version")

// ErrMalformed 表示批次字节流畸形。
var ErrMalformed = errors.New("malformed batch")

// reader 是带严格边界检查的字节游标，越界即判定畸形，不允许静默错列。
type reader struct {
	buf []byte
	off int
	err error
}

func (r *reader) take(n int) ([]byte, bool) {
	if r.err != nil || len(r.buf)-r.off < n {
		r.err = ErrMalformed
		return nil, false
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b, true
}

func (r *reader) u8() byte {
	b, _ := r.take(1)
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

func (r *reader) u16() uint16 {
	b, _ := r.take(2)
	if len(b) < 2 {
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

func (r *reader) u32() uint32 {
	b, _ := r.take(4)
	if len(b) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (r *reader) u64() uint64 {
	b, _ := r.take(8)
	if len(b) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (r *reader) str() string {
	n := int(r.u32())
	b, _ := r.take(n)
	return string(b)
}

func (r *reader) bytes() []byte {
	n := int(r.u32())
	b, _ := r.take(n)
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

type writer struct{ buf []byte }

func (w *writer) u8(v byte)      { w.buf = append(w.buf, v) }
func (w *writer) u16(v uint16)   { w.buf = binary.LittleEndian.AppendUint16(w.buf, v) }
func (w *writer) u32(v uint32)   { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }
func (w *writer) u64(v uint64)   { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }
func (w *writer) str(s string)   { w.u32(uint32(len(s))); w.buf = append(w.buf, s...) }
func (w *writer) bytes(b []byte) { w.u32(uint32(len(b))); w.buf = append(w.buf, b...) }

// writeRecordUnknown 按 tag 升序写出记录级未知字段：
// 每个字段为 tag(u16) | len(u32) | value，前面是字段个数 u16。
// 排序保证同一份输入反复编码的字节流稳定一致、便于逐字节比对。
func (w *writer) recordUnknown(u map[uint16][]byte) {
	tags := make([]uint16, 0, len(u))
	for tag := range u {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	w.u16(uint16(len(tags)))
	for _, tag := range tags {
		w.u16(tag)
		w.bytes(u[tag])
	}
}

// readRecordUnknown 读回记录级未知字段并原样保留。
// 重复标签视为畸形（字段语义不明确，不允许悄悄覆盖）。
func (r *reader) recordUnknown() map[uint16][]byte {
	n := r.u16()
	if r.err != nil || n == 0 {
		return nil
	}
	u := make(map[uint16][]byte, n)
	for i := uint16(0); i < n && r.err == nil; i++ {
		tag := r.u16()
		v := r.bytes()
		if r.err != nil {
			return nil
		}
		if _, dup := u[tag]; dup {
			r.err = fmt.Errorf("%w: duplicate record field tag %d", ErrMalformed, tag)
			return nil
		}
		u[tag] = v
	}
	return u
}

// EncodeBatch 将批次编码为带版本头的字节流。
// v3 布局：
//
//	magic(2) | version(2) | source | nrecords(4) | records... | nunknown(4) | batch-unknown...
//	record = trace(8) | span(8) | level(1) | term(1) | message | termreason
//	       | nattrs(2) {key value}... | rec-unknown
//	rec-unknown = n(2) {tag(2) | len(4) | value}...
//
// 所有 map 均按 key 排序写出，因此同一批次反复编码结果字节级一致。
func EncodeBatch(b Batch) ([]byte, error) {
	w := &writer{}
	w.buf = append(w.buf, magic...)
	w.u16(CurrentVersion)
	w.str(b.Source) // v2 字段
	w.u32(uint32(len(b.Records)))
	for _, rec := range b.Records {
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
		keys := make([]string, 0, len(rec.Attrs))
		for k := range rec.Attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		w.u16(uint16(len(keys)))
		for _, k := range keys {
			w.str(k)
			w.str(rec.Attrs[k])
		}
		w.recordUnknown(rec.Unknown) // v3 字段
	}
	ukeys := make([]string, 0, len(b.Unknown))
	for k := range b.Unknown {
		ukeys = append(ukeys, k)
	}
	sort.Strings(ukeys)
	w.u32(uint32(len(ukeys)))
	for _, k := range ukeys {
		w.str(k)
		w.bytes(b.Unknown[k])
	}
	return w.buf, nil
}

// DecodeBatch 解码批次。
//   - 不支持的版本返回 ErrUnsupportedVersion，明确拒绝而不是按默认值继续；
//   - 畸形数据返回 ErrMalformed，严格边界检查避免静默错列；
//   - 批次级未知字段保留进 Batch.Unknown；v3 记录级未知字段保留进
//     Record.Unknown。两者重编码时均原样写回，不丢失、不错位。
func DecodeBatch(data []byte) (Batch, error) {
	r := &reader{buf: data}
	m, _ := r.take(2)
	if r.err != nil || m[0] != magic[0] || m[1] != magic[1] {
		return Batch{}, fmt.Errorf("%w: bad magic", ErrMalformed)
	}
	version := r.u16()
	if r.err != nil {
		return Batch{}, r.err
	}
	var b Batch
	b.Version = version
	switch version {
	case Version1:
		// v1 无 Source 字段
	case Version2:
		b.Source = r.str()
	case Version3:
		b.Source = r.str()
	default:
		return Batch{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}
	n := r.u32()
	for i := uint32(0); i < n && r.err == nil; i++ {
		rec := Record{
			TraceID:    r.u64(),
			SpanID:     r.u64(),
			Level:      Level(r.u8()),
			Terminated: r.u8() == 1,
			Message:    r.str(),
			TermReason: r.str(),
		}
		nattrs := r.u16()
		if nattrs > 0 {
			rec.Attrs = make(map[string]string, nattrs)
			for j := uint16(0); j < nattrs; j++ {
				k := r.str()
				rec.Attrs[k] = r.str()
			}
		}
		if version >= Version3 {
			rec.Unknown = r.recordUnknown()
		}
		b.Records = append(b.Records, rec)
	}
	if r.err != nil {
		return Batch{}, r.err
	}
	nunknown := r.u32()
	for i := uint32(0); i < nunknown && r.err == nil; i++ {
		k := r.str()
		v := r.bytes()
		if b.Unknown == nil {
			b.Unknown = make(map[string][]byte)
		}
		b.Unknown[k] = v
	}
	if r.err != nil {
		return Batch{}, r.err
	}
	if r.off != len(r.buf) {
		return Batch{}, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(r.buf)-r.off)
	}
	return b, nil
}
