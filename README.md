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

批次编码布局（当前写出 **v3**）：

```
magic(2) | version(2) | source(v2+) | nrecords(4) | records... | batch-unknown
record = trace(8) | span(8) | level(1) | term(1) | message | termreason
       | nattrs(2) {key value}... | rec-unknown(v3+)
rec-unknown = n(2) { tag(2) | len(4) | value }...
```

- 写出版本为 **v3**；**v1** 批次（无 `Source`）与 **v2** 批次（无记录级未知区）
  在新版本中仍可正确读取。v1/v2 数据读入后按 v3 重编码，内容无损。
- **记录级未知字段**（v3）：当前版本读到未来版本给某条记录新增的字段时，
  按字段标签（u16）+ 原始字节保存在 `Record.Unknown`，重新编码时原样写回，
  不丢失、不与相邻字段错位。重复标签判为畸形（语义不明确，不静默覆盖）。
- **批次级未知字段**：批次尾部不认识的字段保存在 `Batch.Unknown`，同样原样保留。
- **确定性编码**：所有 map（attrs、记录级/批次级未知字段）均按 key 排序写出，
  同一份输入反复编码的字节流完全一致；「读旧数据 → 重编码」的结果与原写法
  字节级一致，便于 diff 比对。
- **不支持的版本**返回 `ErrUnsupportedVersion`，明确拒绝而不是按默认值继续。
- **畸形数据**（坏 magic、截断、超长长度、尾部多余字节、重复记录标签）返回
  `ErrMalformed`，严格边界检查避免静默错列。

## 失败分级与丢弃原因

导出失败先区分性质，再决定重试策略：

| 失败性质 | 判定方式 | 行为 |
|----------|----------|------|
| **永久失败**（对方明确不要，再投无用） | 错误被 `Permanent(err)` 包装（`errors.As` 识别），或实现 `PermanentFailure()` 标记方法 | **立即停止重试**，只调用一次接收端 |
| **暂时性失败**（对方暂时不可用） | 其余普通错误 | 指数退避重试，预算用尽才丢弃 |
| **取消/超时** | `context.Canceled` / `context.DeadlineExceeded`（含 `%w` 包装） | 不退避，按取消处理 |

`DropHandler` 回调的丢弃原因四者互斥、可区分：

| 原因 | 字符串 | 含义 |
|------|--------|------|
| `DropQueueFull` | `queue_full` | 队列满，背压丢弃（连尝试都没有） |
| `DropRetryExhausted` | `retry_exhausted` | 暂时性失败重试预算耗尽 |
| `DropCanceled` | `canceled` | 管线关闭或上下文取消 |
| `DropPermanent` | `permanent_rejected` | 接收端明确拒绝，未重试 |

永久失败不占用重试预算、不与 `retry_exhausted` 混淆；`context` 取消错误不会
被误判为永久失败。

## 投递去重保证

管线自身（而非调用方）保证：**同一 `Batch.ID` 的批次无论投递、重试多少次，
包括并发重试，最终在接收端只出现一次可见到达**。实现由有界去重状态表
（`dedupTable`）承担：

- **singleflight 合并**：同一 ID 已有在途导出时，并发调用方作为 follower
  等待 leader 的最终结论，不会各自再打一次接收端；leader 负责整轮退避重试。
  leader 暂时性放弃后，等待者会接管登记重新尝试，不丢批。
- **终结抑制**：导出成功或被永久拒绝后，ID 进入已终结集合；之后再投递同 ID
  直接幂等跳过，不触达接收端，也不重复上报丢弃原因（永久失败只上报一次）。
- **暂时性结论不记忆**：重试耗尽与取消不入集合，后续重新投递仍可正常再试。
- **容量有界（FIFO 淘汰）**：已终结集合最多保留 `Config.DedupCap` 个 ID
  （默认 `DefaultDedupCap = 4096`，`<=0` 取默认值）；超出时按终结顺序
  淘汰最旧条目，长时间运行内存占用有界。被淘汰的 ID 再次投递会被视为新批次，
  即去重窗口覆盖最近 `DedupCap` 个已终结批次——这是有界去重的固有取舍。
  去重状态为进程内状态（与 TraceID 生成一样，不跨进程持久化）。

## 背压与重试策略

- 导出队列容量固定（`Config.QueueSize`），是采集侧的内存开销上限；
  `Submit` 非阻塞，队列满立即丢弃，采集侧绝不无界堆积。
- 暂时性失败按指数退避重试（`Config.Retries` 次）；永久失败不重试。
- 配置通过 `atomic.Pointer` 整体替换：采样、缓存、攒批参数要么全部生效要么
  全部不生效，采样/导出/刷新并发进行时无半更新状态（`go test -race` 验证）。

## 本地验证

```bash
go build ./...        # 编译
go vet ./...          # 静态检查
go test -v ./...      # 单元测试（日志打印输入与判定依据）
go test -race ./...   # 竞态检测（重点覆盖并发去重 singleflight）
```

测试覆盖：

- **失败分级**：`TestPermanentFailureNoRetryAndDistinctReason`（永久失败零重试、
  原因与其他三类互斥）、`TestPermanentFailureByMarkerInterface`（标记方法识别）、
  `TestPermanentFailureClassification`（包装/取消边界）、
  `TestTransientFailureStillRetriesToExhaustion`（暂时性失败照旧退避到耗尽）。
- **去重**：`TestDuplicateSubmissionSuppressed`（重复投递只可见一次）、
  `TestRetryFailureThenRedeliverySucceeds`（暂时性失败后可重投）、
  `TestPermanentDuplicateSuppressedWithoutDoubleDrop`（永久失败重复不二次上报）、
  `TestDedupConcurrentSingleflight` / `TestDedupConcurrentPermanentOnlyOnce`
  （并发场景只产生一次可见到达，`-race` 下验证）、
  `TestDedupFIFOEvictionBounded`（容量上限与 FIFO 淘汰）。
- **字段兼容**：`TestRecordUnknownRoundTrip`（记录级未知字段往返保留）、
  `TestReencodeStable`（反复编码字节级稳定）、`TestFutureRecordFieldsPreserved`
  （未来版本记录字段前向兼容）、`TestV1V2ReadReencodeToV3`（旧版本读取重编码）、
  `TestDuplicateRecordTagRejected`（重复标签判畸形）。
- **既有用例不回归**：取消/超时链路终止标记、慢导出背压、失败重试与去重、
  关闭后投递、v1 旧批次读取、批次级未知字段保留、不支持版本拒绝、畸形批次拒绝、
  并发采集+配置更新+刷新的竞态一致性。

