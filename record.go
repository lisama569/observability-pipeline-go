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
	// Unknown 保留该记录解码时遇到的未知字段（v3 起按长度前缀自描述），
	// 读旧数据再写回时原样保留，不丢失、不错位。
	Unknown map[string][]byte
}
