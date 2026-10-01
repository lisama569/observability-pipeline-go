package pipeline

import "context"

// 本文件以编译期断言锁定既有公共入口的调用形态：新增去重/拒收能力时，
// 按老方式调用这些构造入口的外部代码必须在不改一行的情况下继续编译运行。

// 老版本 NewDispatcher 仍可按 (exp, queueSize, retries, dedupCap, onDrop) 调用。
func init() {
	var _ = func(exp Exporter, onDrop DropHandler) *Dispatcher {
		return NewDispatcher(exp, 8, 2, 64, onDrop)
	}
	// onDrop 可为 nil 的老用法同样保留。
	var _ = func(exp Exporter) *Dispatcher {
		return NewDispatcher(exp, 1, 0, 0, nil)
	}
	// NewPipeline 形态不变。
	var _ = func(cfg Config, exp Exporter, onDrop DropHandler) *Pipeline {
		return NewPipeline(cfg, exp, onDrop)
	}
	// 老版本 Deduper.Begin 返回 (leader, finish(delivered bool))，
	// 旧调用方无需感知三态结论（拒收抑制由管线内部经 BeginEx 完成）。
	var _ = func(dd *Deduper, id uint64) {
		leader, finish := dd.Begin(id)
		if leader {
			finish(true)
		}
	}
	var _ context.Context
}
