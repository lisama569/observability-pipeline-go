package pipeline

import (
	"container/list"
	"sync"
)

// DefaultDedupCap 是批次去重表的默认容量上限。
const DefaultDedupCap = 4096

// Outcome 是一次“主导者”投递结束后的最终结论，决定该 Batch.ID
// 是否进入去重表。
type Outcome int

const (
	// OutcomeUndelivered 未送达：暂时性失败（含重试耗尽）或取消/超时。
	// 不记入去重表，同一个 ID 之后重新投递必须再次正常尝试。
	OutcomeUndelivered Outcome = iota
	// OutcomeDelivered 已成功送达：接收端已可见，ID 记入 LRU，
	// 之后重复投递直接跳过，不再调用接收端。
	OutcomeDelivered
	// OutcomeRejected 被接收端明确永久拒收：ID 记入 LRU，
	// 之后重复投递不再打到接收端，也不再重复上报丢弃。
	OutcomeRejected
)

// Deduper 保证“同一 Batch.ID 投多少次，接收端最终只可见一次”，
// 包含同一批次并发提交、成功送达后再次提交、以及被永久拒收后再次提交。
//
// 去重状态有界：已送达与已拒收 ID 共用一张按 LRU 规则淘汰的表
// （最近最少使用先淘汰），容量由 NewDeduper 的参数指定，
// 长期运行内存不随批次数无限增长。
// 被淘汰的 ID 再次提交会被视为新投递（可能再次到达接收端/再次上报丢弃），
// 这是有界内存下的明确取舍。
type Deduper struct {
	mu       sync.Mutex
	cap      int
	known    map[uint64]*list.Element
	order    *list.List // 队首最近使用，队尾最久未使用
	inflight map[uint64]chan struct{}
}

// knownEntry 是 LRU 链表节点承载的值：ID 及其最终结论。
type knownEntry struct {
	id      uint64
	outcome Outcome
}

// NewDeduper 创建容量受限的去重器；capacity<1 时使用 DefaultDedupCap。
func NewDeduper(capacity int) *Deduper {
	if capacity < 1 {
		capacity = DefaultDedupCap
	}
	return &Deduper{
		cap:      capacity,
		known:    make(map[uint64]*list.Element),
		order:    list.New(),
		inflight: make(map[uint64]chan struct{}),
	}
}

// Begin 登记一次批次投递，返回该次投递是否为“主导者”。
// 这是保留给既有调用方的兼容入口，语义等价于 BeginEx：
//   - 批次已有结论（已送达/已拒收）：leader=false，本次直接跳过；
//   - 同一批次正在投递中：阻塞等待主导者结束后重新判定——主导者
//     未送达时等待者会接管成为新的主导者，失败结论不会吞掉重投；
//   - 否则返回 leader=true，调用方必须在导出结束时执行返回的 finish：
//     delivered=true 记为已送达；false 记为未送达（临时失败/取消）。
//     需要记录“永久拒收”结论的调用方请使用 BeginEx。
func (d *Deduper) Begin(id uint64) (leader bool, finish func(delivered bool)) {
	leader, _, finishEx := d.BeginEx(id)
	return leader, func(delivered bool) {
		if delivered {
			finishEx(OutcomeDelivered)
			return
		}
		finishEx(OutcomeUndelivered)
	}
}

// BeginEx 登记一次批次投递：
//   - 批次已成功送达/已永久拒收过：leader=false，known 携带既有结论，
//     本次既不应再调用导出，也不应再上报丢弃；同时触碰该 ID 的 LRU 位置；
//   - 同一批次正在投递中：阻塞等待主导者结束，之后重新判定：
//     主导者已送达/已拒收则返回 leader=false；主导者未送达则由一个
//     等待者接管成为新的主导者（其余继续等待），保证该重投的不被吞掉、
//     该只到一次的不会到两次；
//   - 否则返回 leader=true、known=OutcomeUndelivered，调用方必须在
//     导出结束时执行 finish 并如实给出结论。
func (d *Deduper) BeginEx(id uint64) (leader bool, known Outcome, finish func(outcome Outcome)) {
	d.mu.Lock()
	for {
		// 已有终态结论：触碰 LRU 后直接判定为重复，不再导出。
		if el, ok := d.known[id]; ok {
			d.order.MoveToFront(el)
			known = el.Value.(knownEntry).outcome
			d.mu.Unlock()
			return false, known, noopFinish
		}
		// 在途：等主导者结束后回到循环开头重新判定。
		if done, ok := d.inflight[id]; ok {
			d.mu.Unlock()
			<-done
			d.mu.Lock()
			continue
		}
		done := make(chan struct{})
		d.inflight[id] = done
		d.mu.Unlock()

		return true, OutcomeUndelivered, func(outcome Outcome) {
			d.mu.Lock()
			delete(d.inflight, id)
			if outcome == OutcomeDelivered || outcome == OutcomeRejected {
				if el, ok := d.known[id]; ok {
					el.Value = knownEntry{id: id, outcome: outcome}
					d.order.MoveToFront(el)
				} else {
					for len(d.known) >= d.cap {
						oldest := d.order.Back()
						if oldest == nil {
							break
						}
						d.order.Remove(oldest)
						delete(d.known, oldest.Value.(knownEntry).id)
					}
					d.known[id] = d.order.PushFront(knownEntry{id: id, outcome: outcome})
				}
			}
			d.mu.Unlock()
			close(done)
		}
	}
}

// noopFinish 是非主导者拿到的空结束回调，重复投递不产生任何状态变更。
func noopFinish(Outcome) {}

// OutcomeOf 返回某 ID 当前记录的终态结论；未记录（从未送达/拒收，
// 或已被 LRU 淘汰）时 ok=false。
func (d *Deduper) OutcomeOf(id uint64) (outcome Outcome, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, exists := d.known[id]; exists {
		return el.Value.(knownEntry).outcome, true
	}
	return OutcomeUndelivered, false
}

// KnownTerminal 非阻塞查询 ID 是否已有终态结论（已送达/已拒收），
// 命中时同时刷新其 LRU 位置。仅供入队前的快速抑制：调用方因此可以
// 不占队列容量地忽略重复投递。在途批次尚无终态，返回 ok=false——
// 并发重复仍需正常入队等待（主导者失败时由等待者接管重试）。
func (d *Deduper) KnownTerminal(id uint64) (outcome Outcome, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, exists := d.known[id]; exists {
		d.order.MoveToFront(el)
		return el.Value.(knownEntry).outcome, true
	}
	return OutcomeUndelivered, false
}

// Len 返回当前记录的终态 ID 数量（已送达 + 已拒收）。
func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.known)
}
