package pipeline

import (
	"container/list"
	"sync"
)

// DefaultDedupCap 是批次去重表的默认容量上限。
const DefaultDedupCap = 4096

// Deduper 保证“同一 Batch.ID 投多少次，接收端最终只可见一次”，
// 包含同一批次并发提交、以及成功送达后再次提交两种场景。
//
// 去重状态有界：已送达 ID 按 LRU 规则淘汰（最近最少使用先淘汰），
// 容量由 NewDeduper 的参数指定，长期运行内存不随批次数无限增长。
// 被淘汰的 ID 再次提交会被视为新投递，这是有界内存下的明确取舍。
type Deduper struct {
	mu        sync.Mutex
	cap       int
	delivered map[uint64]*list.Element
	order     *list.List // 队首最近使用，队尾最久未使用
	inflight  map[uint64]chan struct{}
}

// NewDeduper 创建容量受限的去重器；capacity<1 时使用 DefaultDedupCap。
func NewDeduper(capacity int) *Deduper {
	if capacity < 1 {
		capacity = DefaultDedupCap
	}
	return &Deduper{
		cap:       capacity,
		delivered: make(map[uint64]*list.Element),
		order:     list.New(),
		inflight:  make(map[uint64]chan struct{}),
	}
}

// Begin 登记一次批次投递，返回该次投递是否为“主导者”：
//   - 批次已成功送达过：leader=false，本次直接跳过，不应再调用导出；
//   - 同一批次正在投递中：阻塞等待主导者结束后返回 leader=false（并发去重）；
//   - 否则返回 leader=true，调用方必须在导出结束时执行返回的 finish：
//     delivered=true 表示接收端已可见，ID 记入 LRU；false 表示未送达，
//     不记入去重表，允许后续重新投递。
func (d *Deduper) Begin(id uint64) (leader bool, finish func(delivered bool)) {
	d.mu.Lock()
	// 已送达：触碰 LRU 后直接判定为重复，不再导出。
	if el, ok := d.delivered[id]; ok {
		d.order.MoveToFront(el)
		d.mu.Unlock()
		return false, func(bool) {}
	}
	// 在途：等主导者完成后再查一次送达结论。
	if done, ok := d.inflight[id]; ok {
		d.mu.Unlock()
		<-done
		d.mu.Lock()
		if el, ok := d.delivered[id]; ok {
			d.order.MoveToFront(el)
		}
		d.mu.Unlock()
		return false, func(bool) {}
	}
	done := make(chan struct{})
	d.inflight[id] = done
	d.mu.Unlock()

	return true, func(delivered bool) {
		d.mu.Lock()
		delete(d.inflight, id)
		if delivered {
			if el, ok := d.delivered[id]; ok {
				d.order.MoveToFront(el)
			} else {
				for len(d.delivered) >= d.cap {
					oldest := d.order.Back()
					if oldest == nil {
						break
					}
					d.order.Remove(oldest)
					delete(d.delivered, oldest.Value.(uint64))
				}
				d.delivered[id] = d.order.PushFront(id)
			}
		}
		d.mu.Unlock()
		close(done)
	}
}

// Len 返回当前记录的已送达 ID 数量。
func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.delivered)
}
