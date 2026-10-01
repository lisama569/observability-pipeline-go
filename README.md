# observability-pipeline-go

进程内可观测性采集与导出管线：在一次请求的完整调用链中关联追踪与结构化日志，
在并发派发、上下文取消、导出缓慢与协议版本演进等情况下给出一致且可解释的结论。

## 关联与采样语义

- **关联传播**：`Pipeline.Begin` 开启链路并注入 `Correlation{TraceID, SpanID}`；
  `Pipeline.Derive` 派生子 Span。`Correlation` 是不可变值类型，跨协程派发时
  TraceID 共享、SpanID 唯一，互不串扰（见 `TestCorrelationPropagationAcrossGoroutines`）。
- **终止标记**：上下文被取消或超时后，`CorrelationFrom` 返回的关联被标记为
  `Terminated=true` 并携带 `TermReason`（`canceled`/`timeout`）。此类记录会被
  明确标记导出，而不是静默丢弃。
- **采样**：`Sampler.Decide(traceID)` 是纯函数（FNV-1a 哈希 + 阈值），
  同一链路在多处、多次判定的结论完全一致、可复现。`rate=0` 全不采、`rate=1` 全采。
- **异常保留**：错误级记录与已终止链路始终绕过采样被保留，并进入容量受限的
  FIFO 缓存（`Config.CacheCap`）；容量满时淘汰最旧条目，内存占用有界。

## 错误归类

`Classify` 将错误映射为可区分的类型，底层原因通过 `Unwrap` 保留：

| 类型 | 来源 | 对外透出 |
|------|------|----------|
| `business` | 实现 `BusinessError` 接口 | 归类 + 原因 |
| `canceled` | `context.Canceled`（含 `%w` 包装） | 归类 + 原因 |
| `timeout`  | `context.DeadlineExceeded`（含 `%w` 包装） | 归类 + 原因 |
| `internal` | 其他 | 仅归类（`Error.Public()` 不透出内部细节） |

## 版本兼容规则

批次编码当前写出 **v3**，布局：

```
magic(2) | version(2) | source(v2+) | nrecords(4) | records... | nunknown(4) | unknown...
```

v3 的每条记录为长度前缀自描述分帧：

```
rlen(4) | trace(8) | span(8) | level(1) | term(1)
        | message | termreason | nattrs(2) attrs...
        | nunknown(4) record-unknown...
```

- **读旧数据**：v1（无 `Source`）、v2（有 `Source`、记录不分帧）写出的批次在
  v3 中仍可正确读取；写出时统一升级为 v3。
- **批次级未知字段**：解码时保留进 `Batch.Unknown`，重编码不丢失。
- **记录级未知字段（v3）**：记录帧尾部的未知字段保留进 `Record.Unknown`，
  「读旧数据 → 修改 → 重新编码写回」的往返中原样保留，不丢字段、不错位。
  帧长度前缀把单条记录严格界定，帧内未被完整消耗即判 `ErrMalformed`。
- **记录级字段兼容范围**：`trace` / `span` / `level` / `term` / `message` /
  `termreason` / `attrs` 是 v1 起就存在的固定字段，v1/v2 批次读出时这些
  字段全部可用；记录级 `Record.Unknown` 仅 v3 帧可携带——读 v1/v2 时为
  `nil`，给旧记录追加未知字段后重编码会自动升级为 v3 帧，不影响其他记录。
  `Source`（v2 起）与批次级 `Batch.Unknown` 属于批次头，不随记录帧升级。
- **稳定重编码**：所有 map 型字段（属性、两级未知字段）按 key 排序后写出，
  同一份输入反复编码结果字节级一致，解码后再编码也与原编码一致，便于 diff 比对。
- **不支持的版本**返回 `ErrUnsupportedVersion`，明确拒绝而不是按默认值继续。
- **畸形数据**（坏 magic、截断、超长长度、记录帧内多余字节、尾部多余字节）
  返回 `ErrMalformed`，严格边界检查避免静默错列。

## 导出失败分级与重试

导出端返回的错误先做生命周期判定，再做永久/临时分级：

| 错误情形 | 处理 | 丢弃原因 |
|----------|------|----------|
| 成功 | 结束 | —— |
| 上下文取消 / 超时（含导出返回被包装的 ctx 错误） | 立即停止 | `DropCanceled` |
| **永久失败**：接收端明确拒收（实现 `PermanentError` 或用 `MarkPermanent` 包装） | **立即停止，不重试、不退避等待**，ID 记入去重表 | `DropPermanentlyRejected` |
| **暂时性失败**：其余错误 | 指数退避重试，预算用尽才丢；**不记入去重表** | `DropRetryExhausted` |
| 队列满 / 管线关闭后投递 | 不重试 | `DropQueueFull` / `DropCanceled` |

- `IsPermanent(err)` 识别被 `%w` 包装的永久失败；`context.Canceled` /
  `context.DeadlineExceeded` 永远不算永久失败（那是调用方生命周期终止）。
- 四类丢弃原因字符串互异：`queue_full` / `retry_exhausted` / `canceled` /
  `permanently_rejected`，永久拒绝不再与重试耗尽混报。

## 投递去重（exactly-once 可见）

管线自身（而非调用方）保证「同一 `Batch.ID` 投多少次，接收端最终只可见一次」，
对以下场景都成立：

- 同一批次成功送达后再次提交（含调用方重试式重复投递）；
- 同一批次正在导出时被并发提交（并发重试）——后来者等待主导者结束：
  主导者已送达或已被拒收则直接跳过，不再调用接收端；主导者只是临时失败/
  取消，则等待者中**有且仅有一个**接管成为新主导者继续尝试；
- **同一批次被接收端明确永久拒收后再次提交**：拒收结论是终态，重复投递
  在**入队前**即被静默抑制——不占队列容量、不调用接收端（对端已经明确
  说过不要），也不会产生任何丢弃回调；因此哪怕重投时队列已满，也不会
  再冒出一次 `queue_full`。同一个批次的 `permanently_rejected` 丢弃
  上报只出现一次。

只有**终态**才记入去重表；以下非终态结论不落表，之后重新投递必须正常送出：

- 暂时性失败（重试中、以及重试预算耗尽的 `retry_exhausted`）；
- 取消 / 超时（`canceled`）。

终态结论通过三态 `Deduper.BeginEx` 的 `Outcome` 表达：`OutcomeUndelivered`
（未送达，不落表）/ `OutcomeDelivered`（已送达）/ `OutcomeRejected`（已拒收）。
`Deduper.Begin(id) (leader, finish(delivered bool))` 老入口保持不变，
语义等价于只区分「已送达 / 未送达」；拒收抑制由管线内部自动完成，
既有调用方零改动。

去重状态**有界**：已送达与已拒收 ID 共用容量 `Config.DedupCap`
（`<=0` 用默认 `DefaultDedupCap=4096`）的一张 **LRU** 表，满时淘汰最近
最少使用的 ID（两类结论淘汰规则一致），重复提交会刷新其位置。被淘汰 ID
再次提交视为新批次——可能再次到达接收端、或再次产生一次丢弃上报，
这是有界内存下的明确取舍，容量应按「时间窗口内可能重投的不同批次数」
来配置。

## 背压与其他策略

- 导出队列容量固定（`Config.QueueSize`），是采集侧的内存开销上限；
  `Submit` 非阻塞，队列满立即丢弃，采集侧绝不无界堆积。
- 配置通过 `atomic.Pointer` 整体替换：采样、缓存、攒批参数要么全部生效要么
  全部不生效，采样/导出/刷新并发进行时无半更新状态（`go test -race` 验证）。

## 本地验证

```bash
go build ./...        # 编译
go vet ./...          # 静态检查
go test -v ./...      # 单元测试（日志打印输入与判定依据）
go test -race ./...   # 竞态检测（重点看并发去重用例）
```

重点新增用例：

- `TestPermanentFailureNoRetry` / `TestPermanentFailureReasonDistinct`：
  永久失败只尝试一次、立即以 `permanently_rejected` 丢弃，与其他原因可区分；
- `TestPermanentRejectRedeliverySuppressed`：**被明确拒收的批次串行重投
  4 次，接收端只被调用 1 次，丢弃只上报 1 次**；
- `TestPermanentRejectConcurrentSuppressed`：**同一拒收批次并发投递 16 次
  （主导者导出阻塞期间全部入队），接收端恰好可见一次、丢弃恰好一次**；
- `TestPermanentRejectRedeliveryWhileQueueFull`：队列被慢导出占满时重投
  已拒收批次，入队前静默抑制，不产生 `queue_full`、不产生第二次拒收上报；
- `TestTransientFailureRedelivered` / `TestCanceledBatchRedelivered`：
  临时失败（重试耗尽）与取消/超时（含被包装的 ctx 错误）不记终态，
  同一批次重投正常送达，抑制不波及可重试的失败；
- `TestRedeliverySeenOnce` / `TestConcurrentSubmitSeenOnce`：同一批次重复、
  并发投递，接收端恰好可见一次；
- `TestDeduperRejectedRecorded` / `TestDeduperRejectedVersusDelivered` /
  `TestDeduperLRUEvictionMixedOutcomes` /
  `TestDeduperConcurrentFollowerTakesOver`：拒收结论落表、与已送达结论
  互不混淆、两类结论共用 LRU 容量淘汰、主导者未送达时并发等待者有且仅有
  一个接管（该重投的不被吞、该只到一次的不到两次）；
- `TestDeduperLRUEviction` / `TestDeduperUndeliveredRetriable`：LRU 容量淘汰、
  未送达不入表；
- 编译期兼容断言（`compat_test.go`）：`NewDispatcher(exp, queue, retries,
  cap, onDrop)`、`NewPipeline(cfg, exp, onDrop)`、`Deduper.Begin` 老签名
  原样可调用，老代码不改也能编译运行；
- `TestRecordUnknownFieldsRoundTrip` / `TestReencodeStable`：记录级+批次级
  未知字段往返保留、反复编码字节级稳定；
- `TestMalformedV3RecordRejected`：v3 记录帧截断/多余字节被拒绝。

既有不回归用例：取消链路终止标记（`TestPipelineEndToEnd`）、慢导出背压
（`TestBackpressureQueueFull`、`TestFlushTimeout`）、临时失败重试与耗尽
（`TestRetryNoDuplicateVisible`、`TestRetryExhaustedDrop`）、关闭后投递
（`TestSubmitAfterClose`）、畸形批次（`TestMalformedBatchRejected`）、
v1 旧版本读取（`TestDecodeV1Batch`）、并发采集+配置更新竞态
（`TestPipelineConcurrentRace`）。

用例时序不依赖固定 sleep 判定结论（仅并发场景保留必要的同步等待），
建议提交前至少执行：

```bash
go test -race -count=3 ./...   # 竞态 + 重复运行，稳定可复现
```
