package pipeline

import "sync"

// Sampler 基于链路 ID 做可复现的采样判定。
// 判定只依赖 TraceID 与 rate，因此同一条链路在多处判定时结论一致。
type Sampler struct {
	rate float64
	// threshold 为 rate 映射到 [0, 2^64) 的阈值，避免每次判定做浮点除法。
	threshold uint64
}

func NewSampler(rate float64) Sampler {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return Sampler{rate: rate, threshold: uint64(rate * float64(^uint64(0)))}
}

// hash 使用 FNV-1a 将 TraceID 打散到 [0, 2^64)。
func hash(traceID uint64) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < 8; i++ {
		h ^= (traceID >> (8 * i)) & 0xff
		h *= 1099511628211
	}
	return h
}

// Decide 对同一 TraceID 的多次判定结果一致（纯函数，无内部状态）。
func (s Sampler) Decide(traceID uint64) bool {
	if s.rate >= 1 {
		return true
	}
	if s.rate <= 0 {
		return false
	}
	return hash(traceID) < s.threshold
}

// Cache 是容量受限的异常链路保留缓存。
// 容量满时按 FIFO 淘汰最旧条目，不得无界占用内存。
type Cache struct {
	mu    sync.Mutex
	cap   int
	items map[uint64]struct{}
	order []uint64 // FIFO 队列，队首最旧
}

func NewCache(capacity int) *Cache {
	if capacity < 0 {
		capacity = 0
	}
	return &Cache{cap: capacity, items: make(map[uint64]struct{})}
}

// Keep 保留链路；容量满时淘汰最旧条目。重复保留不占用额外容量。
func (c *Cache) Keep(traceID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[traceID]; ok {
		return
	}
	if c.cap == 0 {
		return
	}
	for len(c.order) >= c.cap {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.items, oldest)
	}
	c.items[traceID] = struct{}{}
	c.order = append(c.order, traceID)
}

// Contains 查询链路是否被保留。
func (c *Cache) Contains(traceID uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[traceID]
	return ok
}

func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
