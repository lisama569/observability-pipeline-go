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
- **批次级未知字段**：解码时保留进 `Batch.Unknown`，重编码写回批次尾部，
  对 v1/v2/v3 读取均适用，作用范围是整个批次。
- **记录级字段的兼容范围（仅 v3）**：
  - 未知字段只存在于 **v3 记录帧尾部**，保留进 `Record.Unknown`；
    v1/v2 记录无分帧、没有记录级未知字段区，旧数据读出来该字段为 `nil`。
  - 「读旧数据 → 修改已知字段 → 重新编码写回」时 v3 的记录级未知字段原样
    保留，不丢字段、不错位；帧长度前缀把单条记录严格界定，帧内未被完整
    消耗即判 `ErrMalformed`，不会把未知字段错读成下一条记录。
  - 记录级未知字段的作用范围是**单条记录**：不与其他记录共享、不与
    `Batch.Unknown` 混淆，且只在解码 v3 字节时填充；直接构造
    `Record{...}` 不涉及兼容问题。
- **稳定重编码**：所有 map 型字段（属性、两级未知字段）按 key 排序后写出，
  同一份输入反复编码结果字节级一致，解码后再编码也与原编码一致，便于 diff 比对。
- **不支持的版本**返回 `ErrUnsupportedVersion`，明确拒绝而不是按默认值继续。
- **畸形数据**（坏 magic、截断、超长长度、记录帧内多余字节、尾部多余字节）
  返回 `ErrMalformed`，严格边界检查避免静默错列。

## 导出失败分级与重试

导出端返回的错误先做生命周期判定，再做永久/临时分级：

| 错误情形 | 处理 | 丢弃原因 |
|----------|------|----------|
| 成功 | 结束，ID 记入去重表 | —— |
| 上下文取消 / 超时（含导出返回被包装的 ctx 错误） | 立即停止 | `DropCanceled` |
| **永久失败**：接收端明确拒收（实现 `PermanentError` 或用 `MarkPermanent` 包装） | **立即停止，不重试、不退避等待**，ID 记入去重表 | `DropPermanentlyRejected` |
| **暂时性失败**：其余错误 | 指数退避重试，预算用尽才丢 | `DropRetryExhausted` |
| 队列满 / 管线关闭后投递 | 不重试 | `DropQueueFull` / `DropCanceled` |

- `IsPermanent(err)` 识别被 `%w` 包装的永久失败；`context.Canceled` /
  `context.DeadlineExceeded` 永远不算永久失败（那是调用方生命周期终止）。
- 四类丢弃原因字符串互异：`queue_full` / `retry_exhausted` / `canceled` /
  `permanently_rejected`，永久拒绝不再与重试耗尽混报。

## 投递去重（exactly-once 可见）与拒收抑制

管线自身（而非调用方）按 `Batch.ID` 去重，每次主导导出结束后得出三态结论：

| 结论 | 触发情形 | 是否记住 ID | 后续重复投递的行为 |
|------|----------|-------------|--------------------|
| 已送达 `OutcomeDelivered` | 接收端返回成功 | 是（LRU） | 静默跳过，接收端不再可见，丢弃计数不变 |
| 已拒收 `OutcomeRejected` | 接收端明确永久拒收 | 是（LRU） | **静默跳过：不再打到接收端，也不会再报一次丢弃**——同一批次的 `permanently_rejected` 全生命周期只出现一次 |
| 未决 `OutcomeUnsettled` | 临时失败重试耗尽、取消、超时 | 否 | 视为新批次正常重新投递，不被去重吞掉 |

对以下场景都成立「同一 `Batch.ID` 投多少次，接收端最终只可见一次」：

- 同一批次成功送达后再次提交（含调用方重试式重复投递）；
- 同一批次被明确拒收后再次提交——对端已表态不要，重投没有意义，
  管线在入队前（`Submit`）即静默抑制，不占队列、不报丢弃；
- 同一批次正在导出时被并发提交（并发重试）——后来者等待主导者结束，
  主导者已送达/已拒收则直接跳过；仅当主导者未决时，后来者也不会
  代替主导者重试（并发重复同样被合并，结论以主导者为准）。

未送达（临时失败重试中、重试耗尽、取消、超时）不记入去重表，
允许后续重新投递；只有成功可见或被明确拒收后才记录该 ID。

### 容量淘汰规则

去重状态**有界**：已送达与已拒收的 ID **共用**一张容量为
`Config.DedupCap`（`<=0` 用默认 `DefaultDedupCap=4096`）的 **LRU** 表，
满时淘汰最近最少使用的 ID，任何重复提交（无论命中的是送达还是拒收结论）
都会刷新其位置。被淘汰 ID 再次提交视为新批次——这是有界内存下的明确取舍，
容量应按「时间窗口内可能重投的不同批次数」来配置；两种终态占用同一容量，
因此持续出现的拒收 ID 与正常送达 ID 一样会按 LRU 被淘汰。

## 接入兼容性

导出侧公共构造入口保持历史位置参数签名，老代码不改也能照常编译与运行：

```go
// 老写法：四个位置参数，去重（含拒收抑制）以默认容量自动启用
disp := pipeline.NewDispatcher(exp, queueSize, retries, onDrop)
// 需要自定义去重容量时再追加可选项
disp := pipeline.NewDispatcher(exp, queueSize, retries, onDrop,
    pipeline.WithDedupCap(1024))
```

新增能力只通过 `DispatcherOption`（如 `WithDedupCap`）提供，
不新增必填位置参数；`NewPipeline` 的签名同样未变，
`Config.DedupCap` 缺省 `0` 时使用默认容量。

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

- `TestRejectedBatchRedeliverySuppressed`：被明确拒收的批次重复投递不再
  打到接收端，`permanently_rejected` 丢弃全生命周期只上报一次，也不混入
  其他丢弃原因；
- `TestConcurrentRejectedSubmissionSeenOnce`：主导者导出期间并发重复投递
  同一批次（最终拒收），接收端只被调用一次、丢弃只报一次；
- `TestTemporaryFailureRedelivered` / `TestCanceledOrTimeoutBatchRedelivered`：
  临时失败重试耗尽、取消/超时没送出去的批次属于未决，之后重新投递正常
  送达一次，不被去重吞掉；
- `TestNewDispatcherLegacySignatureCompiles`：导出侧公共构造入口的老位置
  参数写法不改也能编译运行，新增能力只经可选项提供；
- `TestDeduperRejectedRemembered` / `TestDeduperRejectedSharesLRU`：
  拒收 ID 与送达 ID 一样被记住且结论可区分、共用同一条 LRU 容量淘汰；
- `TestPermanentFailureNoRetry` / `TestPermanentFailureReasonDistinct`：
  永久失败只尝试一次、立即以 `permanently_rejected` 丢弃，与其他原因可区分；
- `TestRedeliverySeenOnce` / `TestConcurrentSubmitSeenOnce`：同一批次重复、
  并发投递，接收端恰好可见一次；
- `TestDeduperLRUEviction` / `TestDeduperUndeliveredRetriable`：LRU 容量淘汰、
  未送达不入表；
- `TestRecordUnknownFieldsRoundTrip` / `TestReencodeStable`：记录级+批次级
  未知字段往返保留、反复编码字节级稳定；
- `TestMalformedV3RecordRejected`：v3 记录帧截断/多余字节被拒绝。

既有不回归用例：取消链路终止标记（`TestPipelineEndToEnd`）、慢导出背压
（`TestBackpressureQueueFull`、`TestFlushTimeout`）、临时失败重试与耗尽
（`TestRetryNoDuplicateVisible`、`TestRetryExhaustedDrop`）、关闭后投递
（`TestSubmitAfterClose`）、畸形批次（`TestMalformedBatchRejected`）、
v1 旧版本读取（`TestDecodeV1Batch`）、并发采集+配置更新竞态
（`TestPipelineConcurrentRace`）。

稳定复现建议：

```bash
go test -race -count=10 ./...   # 并发去重/拒收用例多轮竞态验证
go test -count=20 ./...         # 普通多轮，确认无时序假阳性
```

用例用去重终态轮询（`OutcomeOf`）与 channel 阻塞构造并发窗口，
不依赖固定 sleep 来判定结论，保证在慢机器上也可复现。
