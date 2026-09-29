package pipeline

// Level 表示记录级别。
type Level int

const (
	LevelInfo Level = iota
	LevelWarn
	LevelError
)

// Record 是一条结构化日志或指标记录。
type Record struct {
	TraceID uint64
	SpanID  uint64
	Level   Level
	Message string
	Attrs   map[string]string
	// Terminated 标记该记录产生于已取消/超时的链路之后。
	Terminated bool
	// TermReason 记录链路终止原因（canceled/timeout）。
	TermReason string
	// Unknown 保留记录级别解码时遇到的未知字段，key 为字段标签（u16），
	// value 为该字段的原始字节（TLV 中的 value 部分）。
	// 读旧数据后重新编码时原样写回，既不丢失也不错位；按 key 排序写出保证确定性。
	Unknown map[uint16][]byte
}
