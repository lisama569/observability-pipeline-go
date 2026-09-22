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
}
