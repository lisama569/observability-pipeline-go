// Package sampling provides deterministic per-trace sampling decisions and a
// bounded keep-set for exceptional (error/terminated) chains.
//
// Reproducibility: the ratio sampler derives its verdict from a stable FNV-1a
// hash of the trace id, so the same trace id yields the same verdict at
// every judgment point, across goroutines, for a fixed ratio.
//
// Bounded memory: both the exceptional keep-set and the per-trace decision
// cache are bounded LRU structures; when full, the oldest entry is evicted
// rather than growing without limit. An evicted force-keep id falls back to
// the deterministic sampler verdict on re-judgment, so conclusions remain
// reproducible (they never become arbitrary).
package sampling

import (
	"container/list"
	"context"
	"hash/fnv"
	"sync"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

// Decision is the sampling verdict for a trace.
type Decision int

const (
	DecisionDrop Decision = iota
	DecisionKeep
	// DecisionForceKeep means retained because the chain is exceptional.
	DecisionForceKeep
)

// String returns the stable decision name.
func (d Decision) String() string {
	switch d {
	case DecisionKeep:
		return "keep"
	case DecisionForceKeep:
		return "force_keep"
	default:
		return "drop"
	}
}

// Kept reports whether the decision results in retention.
func (d Decision) Kept() bool { return d == DecisionKeep || d == DecisionForceKeep }

// HashTraceID returns the stable 64-bit hash used for sampling.
func HashTraceID(id trace.TraceID) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(id[:])
	return h.Sum64()
}

// Sampler makes deterministic sampling decisions for a trace.
type Sampler interface {
	Decide(id trace.TraceID) Decision
}

// RatioSampler keeps traces whose normalized hash is below ratio. The same
// trace id always maps to the same verdict for a fixed ratio.
type RatioSampler struct {
	threshold uint64
	ratio     float64
}

// NewRatioSampler returns a ratio sampler; ratio is clamped to [0,1].
func NewRatioSampler(ratio float64) *RatioSampler {
	if ratio <= 0 {
		return &RatioSampler{threshold: 0, ratio: 0}
	}
	if ratio >= 1 {
		return &RatioSampler{threshold: ^uint64(0), ratio: 1}
	}
	// threshold = ratio * 2^64.
	thr := uint64(ratio * float64(1<<63) * 2.0)
	if ratio > 0 && thr == 0 {
		thr = 1
	}
	return &RatioSampler{threshold: thr, ratio: ratio}
}

// Ratio returns the configured sampling ratio.
func (s *RatioSampler) Ratio() float64 { return s.ratio }

// Decide implements Sampler.
func (s *RatioSampler) Decide(id trace.TraceID) Decision {
	if s.threshold == 0 {
		return DecisionDrop
	}
	if HashTraceID(id) < s.threshold {
		return DecisionKeep
	}
	return DecisionDrop
}

// AlwaysSampler keeps every trace.
type AlwaysSampler struct{}

// Decide implements Sampler.
func (AlwaysSampler) Decide(trace.TraceID) Decision { return DecisionKeep }

// lruSet is a bounded set with oldest-entry (LRU) eviction.
type lruSet struct {
	capacity int
	ll       *list.List
	index    map[trace.TraceID]*list.Element
}

func newLRUSet(capacity int) *lruSet {
	if capacity < 1 {
		capacity = 1
	}
	return &lruSet{capacity: capacity, ll: list.New(), index: make(map[trace.TraceID]*list.Element)}
}

// add inserts or refreshes id; returns true if an eviction occurred.
func (l *lruSet) add(id trace.TraceID) bool {
	if el, ok := l.index[id]; ok {
		l.ll.MoveToBack(el)
		return false
	}
	el := l.ll.PushBack(id)
	l.index[id] = el
	if l.ll.Len() > l.capacity {
		front := l.ll.Front()
		l.ll.Remove(front)
		delete(l.index, front.Value.(trace.TraceID))
		return true
	}
	return false
}

func (l *lruSet) contains(id trace.TraceID) bool {
	el, ok := l.index[id]
	if !ok {
		return false
	}
	l.ll.MoveToBack(el)
	return true
}

func (l *lruSet) len() int { return l.ll.Len() }

// KeepSet is a bounded set of exceptional trace ids that must be retained.
// When capacity is exceeded the oldest entry is evicted (fixed LRU policy).
type KeepSet struct {
	set *lruSet
}

// NewKeepSet returns a bounded keep-set of the given capacity (>0).
func NewKeepSet(capacity int) *KeepSet {
	return &KeepSet{set: newLRUSet(capacity)}
}

// Add records an exceptional trace id, evicting the oldest entry if full.
func (k *KeepSet) Add(id trace.TraceID) { k.set.add(id) }

// Contains reports whether the trace id is retained.
func (k *KeepSet) Contains(id trace.TraceID) bool { return k.set.contains(id) }

// Len returns the current number of retained ids.
func (k *KeepSet) Len() int { return k.set.len() }

// Decider combines a sampler with the exceptional keep-set and caches
// per-trace decisions so repeated judgments stay consistent.
type Decider struct {
	mu       sync.Mutex
	sampler  Sampler
	cache    *lruSet2
	keep     *KeepSet
	judgment int64
	kept     int64
	evicted  int64
}

// lruSet2 caches trace id -> Decision.
type lruSet2 struct {
	capacity int
	ll       *list.List
	index    map[trace.TraceID]*list.Element
}

type cacheEntry struct {
	id trace.TraceID
	d  Decision
}

func newLRUCache(capacity int) *lruSet2 {
	if capacity < 1 {
		capacity = 1
	}
	return &lruSet2{capacity: capacity, ll: list.New(), index: make(map[trace.TraceID]*list.Element)}
}

func (c *lruSet2) get(id trace.TraceID) (Decision, bool) {
	if el, ok := c.index[id]; ok {
		c.ll.MoveToBack(el)
		return el.Value.(*cacheEntry).d, true
	}
	return DecisionDrop, false
}

func (c *lruSet2) put(id trace.TraceID, d Decision) bool {
	if el, ok := c.index[id]; ok {
		el.Value.(*cacheEntry).d = d
		c.ll.MoveToBack(el)
		return false
	}
	el := c.ll.PushBack(&cacheEntry{id: id, d: d})
	c.index[id] = el
	if c.ll.Len() > c.capacity {
		front := c.ll.Front()
		c.ll.Remove(front)
		delete(c.index, front.Value.(*cacheEntry).id)
		return true
	}
	return false
}

// DeciderStats summarizes decider behavior.
type DeciderStats struct {
	Judgments int64
	Kept      int64
	Evictions int64
}

// NewDecider builds a decider backed by sampler, a bounded decision cache
// and the exceptional keep-set.
func NewDecider(s Sampler, cacheCapacity int, keep *KeepSet) *Decider {
	if keep == nil {
		keep = NewKeepSet(1)
	}
	return &Decider{sampler: s, cache: newLRUCache(cacheCapacity), keep: keep}
}

// Decide returns a consistent, reproducible decision for id. Exceptional
// ids in the keep-set always force-keep; otherwise the deterministic sampler
// verdict is cached and reused.
func (d *Decider) Decide(ctx context.Context, id trace.TraceID) Decision {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.judgment++
	if dec, ok := d.cache.get(id); ok {
		if dec.Kept() {
			d.kept++
		}
		return dec
	}
	dec := d.sampler.Decide(id)
	if d.keep.Contains(id) {
		dec = DecisionForceKeep
	}
	if d.cache.put(id, dec) {
		d.evicted++
	}
	if dec.Kept() {
		d.kept++
	}
	return dec
}

// NoteExceptional marks an existing chain as exceptional: it joins the
// bounded keep-set and any cached verdict for it is upgraded to force-keep
// so subsequent judgments agree.
func (d *Decider) NoteExceptional(id trace.TraceID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.keep.Add(id)
	if d.cache.put(id, DecisionForceKeep) {
		d.evicted++
	}
}

// UpdateSampler atomically replaces the underlying sampler. Existing cached
// verdicts are left intact so previously judged traces keep a consistent
// conclusion; not-yet-judged traces use the new sampler. The swap itself is
// fully synchronized, so concurrent decisions never see a half-updated state.
func (d *Decider) UpdateSampler(s Sampler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sampler = s
}

// Stats returns a snapshot of decider counters.
func (d *Decider) Stats() DeciderStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return DeciderStats{Judgments: d.judgment, Kept: d.kept, Evictions: d.evicted}
}
