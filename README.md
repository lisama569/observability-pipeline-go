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

批次编码布局：`magic(2) | version(2) | [source(v2)] | records | unknown`。

- 当前写出版本为 **v2**（新增 `Source` 字段）；**v1** 写出的批次在新版本中
  仍可被正确读取（`Source` 置空）。
- 解码遇到**未知字段**时保留进 `Batch.Unknown`，重编码不丢失。
- **不支持的版本**返回 `ErrUnsupportedVersion`，明确拒绝而不是按默认值继续。
- **畸形数据**（坏 magic、截断、超长长度、尾部多余字节）返回 `ErrMalformed`，
  严格边界检查避免静默错列。

## 背压与丢弃策略

- 导出队列容量固定（`Config.QueueSize`），是采集侧的内存开销上限；
  `Submit` 非阻塞，队列满立即丢弃，采集侧绝不无界堆积。
- 导出失败按指数退避重试（`Config.Retries` 次）；重试针对同一 `Batch.ID`，
  接收端按 ID 去重，不产生重复可见数据。
- 丢弃必须携带可区分原因并回调 `DropHandler`：
  `DropQueueFull`（背压）、`DropRetryExhausted`（重试耗尽）、`DropCanceled`（关闭/取消）。
- 配置通过 `atomic.Pointer` 整体替换：采样、缓存、攒批参数要么全部生效要么
  全部不生效，采样/导出/刷新并发进行时无半更新状态（`go test -race` 验证）。

## 本地验证

```bash
go build ./...        # 编译
go vet ./...          # 静态检查
go test -v ./...      # 单元测试（日志打印输入与判定依据）
go test -race ./...   # 竞态检测
```

测试覆盖：取消/超时链路的终止标记、慢速导出端背压、失败导出端重试与去重、
重试耗尽丢弃、关闭后投递、v1 旧批次读取、未知字段保留、不支持版本拒绝、
畸形批次拒绝、并发采集+配置更新+刷新的竞态一致性。
