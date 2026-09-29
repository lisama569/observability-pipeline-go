package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Config 是管线配置，整体原子替换以避免半更新状态。
type Config struct {
	// SampleRate 采样率 [0,1]，判定对同一 TraceID 可复现。
	SampleRate float64
	// CacheCap 异常链路保留缓存容量（FIFO 淘汰）。
	CacheCap int
	// QueueSize 导出队列容量，是采集侧的内存开销上限。
	QueueSize int
	// Retries 导出失败时的最大重试次数。
	Retries int
	// DedupCap 批次投递去重表容量（LRU 淘汰），<=0 时使用默认值。
	DedupCap int
	// BatchSize 攒批大小，达到即投递。
	BatchSize int
}

func (c Config) normalized() Config {
	if c.SampleRate < 0 {
		c.SampleRate = 0
	}
	if c.SampleRate > 1 {
		c.SampleRate = 1
	}
	if c.CacheCap < 0 {
		c.CacheCap = 0
	}
	if c.QueueSize < 1 {
		c.QueueSize = 1
	}
	if c.Retries < 0 {
		c.Retries = 0
	}
	if c.BatchSize < 1 {
		c.BatchSize = 1
	}
	return c
}

// Pipeline 是采集与导出管线的入口。
type Pipeline struct {
	cfg  atomic.Pointer[Config]
	ids  idGenerator
	disp *Dispatcher

	cacheMu sync.Mutex
	cache   *Cache

	bufMu sync.Mutex
	buf   []Record
}

func NewPipeline(cfg Config, exp Exporter, onDrop DropHandler) *Pipeline {
	cfg = cfg.normalized()
	p := &Pipeline{
		disp:  NewDispatcher(exp, cfg.QueueSize, cfg.Retries, cfg.DedupCap, onDrop),
		cache: NewCache(cfg.CacheCap),
	}
	p.cfg.Store(&cfg)
	return p
}

// Config 返回当前配置快照。
func (p *Pipeline) Config() Config { return *p.cfg.Load() }

// UpdateConfig 原子替换整个配置指针：采样、缓存与攒批参数要么全部生效，
// 要么全部不生效，不会出现半更新状态。
func (p *Pipeline) UpdateConfig(cfg Config) {
	cfg = cfg.normalized()
	p.cfg.Store(&cfg)
	p.cacheMu.Lock()
	if p.cache == nil || p.cache.cap != cfg.CacheCap {
		p.cache = NewCache(cfg.CacheCap)
	}
	p.cacheMu.Unlock()
}

// Start 启动导出消费循环。
func (p *Pipeline) Start(ctx context.Context) { p.disp.Start(ctx) }

// Begin 开启一条新链路，返回注入关联信息的上下文。
func (p *Pipeline) Begin(ctx context.Context) context.Context {
	id := p.ids.Next()
	return WithCorrelation(ctx, Correlation{TraceID: id, SpanID: id})
}

// Derive 在已有链路上派生子 Span，供跨协程派发使用。
func (p *Pipeline) Derive(ctx context.Context) context.Context {
	c := CorrelationFrom(ctx)
	return WithCorrelation(ctx, c.Child(p.ids.Next()))
}

// Emit 采集一条记录：关联上下文、采样判定、攒批投递。
// 上下文已取消/超时的记录被明确标记为已终止链路，而不是静默丢弃。
func (p *Pipeline) Emit(ctx context.Context, level Level, msg string, attrs map[string]string) {
	cfg := p.cfg.Load()
	corr := CorrelationFrom(ctx)
	rec := Record{
		TraceID:    corr.TraceID,
		SpanID:     corr.SpanID,
		Level:      level,
		Message:    msg,
		Attrs:      attrs,
		Terminated: corr.Terminated,
		TermReason: corr.TermReason,
	}
	// 采样判定是纯函数，同一条链路在多处判定结论一致。
	// 异常（错误级或已终止）链路始终保留并记入有界缓存。
	abnormal := level == LevelError || corr.Terminated
	if !abnormal && !cfg.Sampler().Decide(rec.TraceID) {
		return
	}
	if abnormal {
		p.cacheMu.Lock()
		p.cache.Keep(rec.TraceID)
		p.cacheMu.Unlock()
	}
	p.bufMu.Lock()
	p.buf = append(p.buf, rec)
	full := len(p.buf) >= cfg.BatchSize
	var batch []Record
	if full {
		batch = p.buf
		p.buf = nil
	}
	p.bufMu.Unlock()
	if full {
		p.disp.Submit(Batch{ID: p.ids.Next(), Version: CurrentVersion, Records: batch})
	}
}

// Kept 查询链路是否因异常被保留。
func (p *Pipeline) Kept(traceID uint64) bool {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	return p.cache.Contains(traceID)
}

// Flush 把缓冲区残余记录投递出去，并等待在途批次完成或 ctx 到期。
func (p *Pipeline) Flush(ctx context.Context) error {
	p.bufMu.Lock()
	if len(p.buf) > 0 {
		batch := p.buf
		p.buf = nil
		p.bufMu.Unlock()
		p.disp.Submit(Batch{ID: p.ids.Next(), Version: CurrentVersion, Records: batch})
	} else {
		p.bufMu.Unlock()
	}
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for p.disp.Inflight() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

// Close 刷新并关闭管线。
func (p *Pipeline) Close(ctx context.Context) error {
	err := p.Flush(ctx)
	p.disp.Close()
	return err
}

// Sampler 由配置派生采样器。
func (c Config) Sampler() Sampler { return NewSampler(c.SampleRate) }
