# 进程内可观测性采集与导出管线（Go）

一套零第三方依赖的进程内可观测性管线：在**并发派发、上下文取消/超时、导出缓慢或失败、协议版本演进**等条件下，给出一致、可解释、可追溯的结论。

模块路径：`github.com/lisama569/observability-pipeline-go`，要求 Go 1.25+。

## 包结构

| 包 | 职责 |
| --- | --- |
| `internal/observe/errkind` | 错误归类（业务/取消/超时/内部），保留底层原因，对内部错误消毒 |
| `internal/observe/trace` | TraceID/SpanID、上下文关联、结构化记录、跨协程传播、终止标记 |
| `internal/observe/sampling` | 确定性采样（FNV-1a 哈希）+ 有界 LRU 异常链路保留集与决策缓存 |
| `internal/observe/wire` | 带显式版本号的批次 JSON 编解码，未知字段保留，不支持版本明确拒绝 |
| `internal/observe/export` | 有界队列、批处理、背压、有界重试、接收端去重、可区分的丢弃原因 |
| `internal/observe/pipeline` | 顶层装配（归类→采样→导出），整体配置原子切换 |
| `example` | 可运行端到端示例 |

## 1. 关联与跨协程传播语义

- 每条请求链路有一个 `TraceID`（16 字节随机），每个 span 有 `SpanID`（8 字节随机）和 `ParentID`。关联信息以**值**的形式存放在 `context.Context` 中。
- `trace.StartSpan(ctx, name)` 在当前关联下建立子关联并返回新 `ctx` 与 `*Span`；不存在关联时建立根链路。
- `trace.Detach(ctx, name, fn)` 把**当前关联的独立副本**、sink 与父 `ctx` 的只读引用显式传入新协程：
  - 协程间不共享可变关联，因此并发派发不会**串扰**；
  - 分离协程使用 `context.Background()` 派生，父级取消**不会**强行终止后台工作，不会**丢失**派发出去的关联；
  - 但分离协程在产生记录时仍会观察**原始父 ctx** 的终止状态。
- **取消/超时不静默丢弃**：`ctx.Err()` 为 `context.Canceled` / `context.DeadlineExceeded` 时（含被 `fmt.Errorf("...: %w", ctxErr)` 包装），新产生的记录照样发出，但被显式标记
  - `Record.Terminated = true`
  - `Record.TermReason = "canceled" | "deadline_exceeded"`
- `Span.End` 至多生效一次（幂等），结束时对错误进行归类并写出终态记录。

## 2. 错误归类语义

`errkind.Classify(err)`（先判定超时，再判定取消，再判定业务，其余为内部错误）：

| 类别 | 来源 | 对外结论 | 底层原因 |
| --- | --- | --- | --- |
| `business` | `errkind.Business(...)` / `Businessf` | **原样透出**（调用方可见） | `Unwrap` 可追溯 |
| `canceled` | `context.Canceled`（含包装） | 稳定文案 `request canceled` | `Unwrap` 保留原错误 |
| `deadline_exceeded` | `context.DeadlineExceeded`（含包装） | 稳定文案 `request timed out` | `Unwrap` 保留原错误 |
| `internal` | 任何其他非空错误 | **统一文案 `internal error`**，绝不透出实现细节 | `Unwrap` 保留原错误供内部诊断 |

- 取消与超时同时可判定时，超时是更具体的结论（优先）。
- `errkind.Sanitize(err)` 返回安全的对外错误；`errors.Is/As/Unwrap` 仍能触达原始原因。
- 已归类的错误再次 `Classify` 不会改变类别（结论幂等）。
- 追踪记录上的 `Record.ErrorKind` 即为该记录的错误类别字段。

## 3. 采样与异常保留语义

- 比例采样器对 `TraceID` 做稳定的 FNV-1a 64 位哈希，与阈值比较：
  - **同一条链路、同一比例，结论在任意判定点、任意协程上都一致且可复现**；
  - 比例被夹取到 `[0,1]`；`0` 全丢、`1`（或 >1）全留。
- `Decider` 用有界 LRU 缓存「trace id → 结论」，重复判定直接复用；并发判定在同一把锁下保持一致。
- **异常链路强制保留**：终止（取消/超时）或带任何错误类别的链路，经 `NoteExceptional` 进入有界保留集，判定为 `force_keep`，即使比例为 0 也导出；已缓存的结论会被升级为 `force_keep`，保证后续判定不回退。
- **容量受限**：异常保留集与决策缓存都是有界 LRU，满了淘汰**最久未使用**条目，绝不无界增长。被淘汰的 id 若再次出现，会退回到确定性采样器的可复现结论，而不是随机结果。
- 被采样丢弃的普通记录只计数（`Stats.SampledOut`），**不进入**导出队列，因此导出队列只承载真正要导出的数据。

## 4. 版本兼容规则（`wire`）

批次采用带版本的信封：

```json
{ "v": 1, "batch_id": "...", "records": [ { "trace_id": "...", ... } ] }
```

- `v` 是必填显式版本。仅接受 `MinSupportedVersion..CurrentVersion`（当前均为 1）；区间外或缺版本号一律返回 `ErrUnsupportedVersion`，**绝不按默认值继续读取**。
- **旧版本写出的批次**：v1 载荷缺少新版本可选字段时，缺失字段取零值，仍可正确读取（前向读取）。
- **未知字段**：信封级与记录级的未知字段在解码时被逐字保留到 `Batch.Unknown` / `Record.Unknown`，再次编码时原样写回；不会静默错列或丢失。
- 结构性/类型/解析错误（非法 JSON、非法 trace/span id、无法解析的时间、records 不是数组等）返回 `ErrMalformed`，不会产生可能被误认为合法的半成品。
- 新版本发布时：新增字段天然被旧读者保留；若语义不兼容则提升 `CurrentVersion`/`MinSupportedVersion`，旧版本被明确拒绝。

## 5. 背压、重试与丢弃策略（`export`）

**硬内存上限，采集侧绝不无界堆积：**

- `QueueCapacity` 限定待导出记录数。`Submit` **永不阻塞**；队列满时按策略处理并回调 `DropNotice`：
  - `DropNewest`（默认）：拒绝新来的记录，保留已排队内容；
  - `DropOldest`：淘汰队首记录以接纳新记录。

**有界重试，接收端不产生重复可见数据：**

- 批次达到 `BatchSize` 或 `FlushInterval` 到期即导出；单批尝试次数 ≤ `MaxAttempts`，线性退避 `Backoff * attempt`，单次尝试受 `ExportTimeout` 约束。
- 每次重试使用**同一个 `BatchID`**；外层 `DedupExporter` 维护有界的「已接受 BatchID」LRU 集合，同一批次重投直接成功应答、不再下发，因此接收端最多看到一次。
- 失败区分：`*ExportError` 用 `Retryable()` 标明；`Retryable(...)` 会重试，`Permanent(...)`（含畸形/不支持版本载荷）只尝试一次。上下文取消/超时不重试。未知错误按永久处理，避免意外重试风暴。

**丢弃必须留下可区分原因：**

| `DropReason` | 触发条件 |
| --- | --- |
| `queue_full` | 有界队列满，背压策略丢弃 |
| `retry_exhausted` | 暂时性失败重试次数用尽 |
| `payload_rejected` | 后端永久拒绝（含编码/畸形载荷） |
| `shutdown` | 关闭时仍未送达 |

`Shutdown` 不再接收新记录（新提交得到 `shutdown` 丢弃），取消后台 worker，用传入 `ctx` 做一次有时限的末次 flush，剩余数据以 `shutdown` 原因丢弃——不会无限阻塞。

## 6. 性能开销上限与配置一致性

- 队列容量、批次大小、刷新间隔、导出超时、重试次数、决策缓存、保留集容量都是有界配置项，构成明确的开销上限。
- `Batcher.UpdateConfig` / `Pipeline.UpdateConfig` 以**整体 Config 原子替换**（指针原子写），采样器替换在 `Decider` 同一把锁内完成；并发的采样、导出、刷新绝不会读到半更新状态。
- 计数器使用原子量；`Stats()` 返回一致快照。该不变量在 `go test -race` 下持续验证。

## 7. 本地验证方法

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全量单元测试（含输入与判定依据日志）
go test -v -count=1 ./...

# 竞态检测（重点：采样/导出/刷新并发 + 中途改配置）
go test -race -count=1 ./...

# 端到端可运行示例
go run ./example
```

测试重点与需求对应：

- **取消链路**：取消/超时后产生的记录被标记 `terminated` 且仍导出；`Detach` 协程不被父取消杀死但能观察父终止；取消错误归类为 `canceled`。
- **慢/失败导出端**：慢导出下队列不超过上限并产生 `queue_full`；连续 503 在重试耗尽后以 `retry_exhausted` 丢弃；400 不重试，按 `payload_rejected` 丢弃；同一 `BatchID` 重试在去重器下只到达后端一次。
- **畸形批次/不支持版本**：`v=0/v=2/缺 v/版本类型错/非法 JSON/坏 id/坏时间` 分别被拒绝为 `ErrUnsupportedVersion` 或 `ErrMalformed`，未知字段保留并可往返。
- **失败不污染已有状态**：永久/重试失败不影响已接受批次；`Span.End` 幂等；Shutdown 后提交得到明确丢弃而非挂起。
- 每个测试用例都会用 `t.Logf` 打印**输入**与**判定依据**，`go test -v` 可直接查看。
