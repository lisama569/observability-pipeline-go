package pipeline

import (
	"context"
	"sync"
)

// Correlation 是跨协程传播的关联信息。
// 值类型 + 不可变使用方式保证并发派发的协程之间不串扰。
type Correlation struct {
	TraceID    uint64
	SpanID     uint64
	Terminated bool
	TermReason string
}

// Child 派生一个新的子 Span 关联，TraceID 不变。
func (c Correlation) Child(spanID uint64) Correlation {
	return Correlation{TraceID: c.TraceID, SpanID: spanID}
}

type ctxKey struct{}

// WithCorrelation 将关联信息注入上下文。
func WithCorrelation(ctx context.Context, c Correlation) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// CorrelationFrom 从上下文中取出关联信息。
// 若上下文已取消或超时，返回的关联被明确标记为已终止链路，
// 并给出可区分的原因（canceled/timeout），而不是静默丢弃。
func CorrelationFrom(ctx context.Context) Correlation {
	c, _ := ctx.Value(ctxKey{}).(Correlation)
	if err := ctx.Err(); err != nil {
		ce := Classify(err)
		c.Terminated = true
		c.TermReason = ce.Kind.String()
	}
	return c
}

// idGenerator 生成进程内唯一的 TraceID/SpanID。
type idGenerator struct {
	mu   sync.Mutex
	next uint64
}

func (g *idGenerator) Next() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return g.next
}
