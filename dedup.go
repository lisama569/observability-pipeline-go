package pipeline

import (
	"context"
	"sync"
)

// DefaultDedupCap 是去重状态的默认容量上限：
// 最多记住最近 4096 个已终结（成功送达或被永久拒绝）的批次 ID。
const DefaultDedupCap = 4096

// dedupOutcome 表示一次去重登记的结论。
type dedupOutcome int

const (
	// outcomeLeader 调用方负责执行本次导出（含整轮重试）。
	outcomeLeader dedupOutcome = iota
	// outcomeFollower 同一批次正由其他调用方导出，本调用方等待其最终结论。
	outcomeFollower
	// outcomeDup 同一批次此前已终结（送达或永久拒绝），本次为幂等重复，直接跳过。
	outcomeDup
)

// dedupAttempt 表示同一批次 ID 的一次在途导出协调。
// leader 写结论后 close(done)；follower 阻塞等待，实现并发重试的 singleflight。
type dedupAttempt struct {
	done      chan struct{}
	err       error
	finalized bool // true=已终结（成功/永久拒绝），false=暂时性放弃（失败耗尽/取消）
}

// wait 等待 leader 的最终结论。
// 返回 finalized=true 表示批次已有终结结论（follower 无需任何动作）；
// ctx 提前取消时返回 ctx.Err()，批次命运仍由 leader 负责。
func (a *dedupAttempt) wait(ctx context.Context) (bool, error) {
	select {
	case <-a.done:
		return a.finalized, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// dedupTable 是有界的批次去重状态表。
//
// 状态分两类：
//   - live：正在导出的批次，多个并发调用在此合并为一次实际导出（singleflight）；
//   - seen：已终结批次（成功送达或被永久拒绝），重复投递直接抑制，不再触达接收端。
//
// seen 按终结顺序做 FIFO 淘汰：超过容量时移除最旧条目，保证长时间运行时
// 内存占用有界（上限 = live 数 + cap）。被淘汰后再次投递同一 ID 将被当作新
// 批次重新导出——这是有界去重的固有取舍，窗口覆盖最近 cap 个已终结批次。
// 暂时性失败（重试耗尽）与取消不进入 seen，后续重新投递仍可正常重试。
type dedupTable struct {
	mu   sync.Mutex
	cap  int
	live map[uint64]*dedupAttempt
	seen map[uint64]struct{}
	ord  []uint64 // seen 的 FIFO 顺序，队首最旧
}

func newDedupTable(capacity int) *dedupTable {
	if capacity <= 0 {
		capacity = DefaultDedupCap
	}
	return &dedupTable{
		cap:  capacity,
		live: make(map[uint64]*dedupAttempt),
		seen: make(map[uint64]struct{}),
	}
}

// begin 登记一次对批次 id 的导出，返回角色与对应的在途协调对象。
func (t *dedupTable) begin(id uint64) (*dedupAttempt, dedupOutcome) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.seen[id]; ok {
		return nil, outcomeDup
	}
	if a, ok := t.live[id]; ok {
		return a, outcomeFollower
	}
	a := &dedupAttempt{done: make(chan struct{})}
	t.live[id] = a
	return a, outcomeLeader
}

// finalize 记录一个终结结论（成功或永久拒绝），并入 seen 接受容量淘汰。
func (t *dedupTable) finalize(id uint64, a *dedupAttempt, err error) {
	a.err = err
	a.finalized = true
	t.mu.Lock()
	delete(t.live, id)
	if _, ok := t.seen[id]; !ok {
		for len(t.ord) >= t.cap {
			oldest := t.ord[0]
			t.ord = t.ord[1:]
			delete(t.seen, oldest)
		}
		t.seen[id] = struct{}{}
		t.ord = append(t.ord, id)
	}
	t.mu.Unlock()
	close(a.done)
}

// abandon 记录一个非终结结论（暂时性失败耗尽或取消），不进入 seen。
func (t *dedupTable) abandon(id uint64, a *dedupAttempt, err error) {
	a.err = err
	a.finalized = false
	t.mu.Lock()
	delete(t.live, id)
	t.mu.Unlock()
	close(a.done)
}
