package pipeline

import (
	"container/list"
	"sync"
)

// DefaultDedupCap 是批次去重表的默认容量上限。
const DefaultDedupCap = 4096

// DedupOutcome 表示一次主导导出的最终结论，决定该批次 ID 是否被去重表记住。
type DedupOutcome int

const (
	// OutcomeUnsettled 未形成终态：暂时性失败重试耗尽、上下文取消/超时。
	// 不记入去重表，后续重新投递仍会正常打到接收端。
	OutcomeUnsettled DedupOutcome = iota
	// OutcomeDelivered 已成功送达：后续重复投递直接跳过，接收端只可见一次。
	OutcomeDelivered
	// OutcomeRejected 接收端明确永久拒收：后续重复投递不再打到接收端，
	// 也不会因为重投再上报一次丢弃——同一批次的丢弃只上报一次。
	OutcomeRejected
)

// dedupEntry 记录一个已形成终态的批次 ID 及其结论。
type dedupEntry struct {
	id      uint64
	outcome DedupOutcome
}

// Deduper 保证“同一 Batch.ID 投多少次，接收端最终只可见一次”，
// 并对“接收端已明确拒收”的批次在后续重复投递时静默抑制。
//
// 去重状态有界：已送达与已拒收的 ID 共用一条 LRU（最近最少使用先淘汰），
// 容量由 NewDeduper 的参数指定，长期运行内存不随批次数无限增长。
// 被淘汰的 ID 再次提交会被视为新投递，这是有界内存下的明确取舍。
type Deduper struct {
	mu       sync.Mutex
	cap      int
	settled  map[uint64]*list.Element // 已送达 + 已拒收的终态 ID
	order    *list.List               // 队首最近使用，队尾最久未使用
	inflight map[uint64]chan struct{}
}

// NewDeduper 创建容量受限的去重器；capacity<1 时使用 DefaultDedupCap。
func NewDeduper(capacity int) *Deduper {
	if capacity < 1 {
		capacity = DefaultDedupCap
	}
	return &Deduper{
		cap:      capacity,
		settled:  make(map[uint64]*list.Element),
		order:    list.New(),
		inflight: make(map[uint64]chan struct{}),
	}
}

// Begin 登记一次批次投递，返回该次投递是否为“主导者”：
//   - 批次已成功送达或已被明确拒收：leader=false，本次直接跳过，
//     不应再调用导出，也不应再上报丢弃；
//   - 同一批次正在投递中：阻塞等待主导者结束后返回 leader=false（并发去重）；
//   - 否则返回 leader=true，调用方必须在导出结束时执行返回的 finish，
//     传入终态结论：已送达/已拒收会被记入 LRU；未决（临时失败、取消、
//     超时）不记入，允许后续重新投递。
func (d *Deduper) Begin(id uint64) (leader bool, finish func(DedupOutcome)) {
	d.mu.Lock()
	// 已有终态：触碰 LRU 后直接判定为重复，不再导出。
	if el, ok := d.settled[id]; ok {
		d.order.MoveToFront(el)
		d.mu.Unlock()
		return false, func(DedupOutcome) {}
	}
	// 在途：等主导者完成后再查一次终态结论。
	if done, ok := d.inflight[id]; ok {
		d.mu.Unlock()
		<-done
		d.mu.Lock()
		if el, ok := d.settled[id]; ok {
			d.order.MoveToFront(el)
		}
		d.mu.Unlock()
		return false, func(DedupOutcome) {}
	}
	done := make(chan struct{})
	d.inflight[id] = done
	d.mu.Unlock()

	return true, func(outcome DedupOutcome) {
		d.mu.Lock()
		delete(d.inflight, id)
		if outcome != OutcomeUnsettled {
			if el, ok := d.settled[id]; ok {
				el.Value.(*dedupEntry).outcome = outcome
				d.order.MoveToFront(el)
			} else {
				for len(d.settled) >= d.cap {
					oldest := d.order.Back()
					if oldest == nil {
						break
					}
					d.order.Remove(oldest)
					delete(d.settled, oldest.Value.(*dedupEntry).id)
				}
				d.settled[id] = d.order.PushFront(&dedupEntry{id: id, outcome: outcome})
			}
		}
		d.mu.Unlock()
		close(done)
	}
}

// IsSettled 非阻塞查询 ID 是否已有终态（已送达或已拒收）；
// 命中时触碰其 LRU 位置。Submit 路径用它在入队前静默抑制重复批次，
// 使被拒收批次的重复投递既不到达接收端，也不占用队列或重复上报丢弃。
func (d *Deduper) IsSettled(id uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.settled[id]; ok {
		d.order.MoveToFront(el)
		return true
	}
	return false
}

// OutcomeOf 返回 ID 的终态结论；尚未形成终态时 ok=false。
func (d *Deduper) OutcomeOf(id uint64) (outcome DedupOutcome, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, exists := d.settled[id]; exists {
		return el.Value.(*dedupEntry).outcome, true
	}
	return OutcomeUnsettled, false
}

// Len 返回当前记录的终态（已送达 + 已拒收）ID 数量。
func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.settled)
}
