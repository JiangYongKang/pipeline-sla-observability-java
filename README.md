# Pipeline SLA Observability（流水线运行状态接入与 SLA 监控服务）

基于 Spring Boot 3 / Java 21 的本地服务：接入流水线运行事件，在**并发提交、乱序到达、
重复投递**下给出一致、可解释的运行状态、新鲜度/SLA 判定与失败重试指标，全部数据保存在
本地（内存 + append-only JSONL 快照），无外部依赖。

---

## 1. 事件模型

### 1.1 事件结构 `PipelineEvent`

| 字段 | 含义 |
|---|---|
| `eventId` | 事件全局唯一 ID，用于重复提交去重 |
| `runId` | 一次调度运行的唯一 ID；**多次重试共享同一 runId** |
| `pipelineName` | 流水线名称（聚合维度、SLA 策略维度） |
| `type` | `SCHEDULED` / `STARTED` / `SUCCEEDED` / `FAILED` |
| `attempt` | 尝试序号，从 1 开始（`SCHEDULED` 传 0） |
| `eventTime` | 事件在产生端发生的时间（epoch millis） |
| `retryable` | 仅 `FAILED`：该失败是否可重试 |
| `failureReason` | 仅 `FAILED`：失败原因，必填 |
| `itemCount` | 本次尝试处理条目数，用于吞吐计算（可选） |

### 1.2 合法生命周期与运行聚合状态

```
SCHEDULED ──STARTED(a=1)──► RUNNING ──SUCCEEDED──────────► SUCCEEDED（终态）
                               │
                               └──FAILED(retryable=true,
                                        未达 maxAttempts)──► AWAITING_RETRY
                                        │
                                        └──STARTED(a=2)──► RUNNING …

RUNNING/AWAITING_RETRY ──FAILED(retryable=false 或已达 maxAttempts)──► FAILED_FINAL（终态）
```

`RunState`：`SCHEDULED / RUNNING / AWAITING_RETRY / SUCCEEDED / FAILED_FINAL`。

### 1.3 拒绝原因分类（绝不静默接受或覆盖）

任何不满足规则的事件都返回 `accepted=false` 与明确原因（HTTP 422）：

| `RejectReason` | 触发条件 |
|---|---|
| `DUPLICATE_EVENT` | `eventId` 已接受过（重复提交返回 200 且 `duplicate=true`，幂等不重复计数；同一 eventId 绑定别的 runId 的串线也拒绝） |
| `UNKNOWN_RUN` | 未收到 `SCHEDULED` 就收到生命周期事件 |
| `FIRST_EVENT_MUST_BE_SCHEDULED` | 保留：首个事件必须是 SCHEDULED |
| `ILLEGAL_TRANSITION` | 状态回退（如 STARTED 后再来 SCHEDULED）、超过最大尝试次数仍 STARTED、上一次尝试尚未结束就开始下一次 |
| `TERMINAL_RUN` | 运行已 `SUCCEEDED`/`FAILED_FINAL`，终态后的一切事件（含重复 SCHEDULED、迟到完成事件） |
| `ATTEMPT_MISMATCH` | 完成事件 attempt ≠ 当前进行中的尝试号；STARTED attempt ≠ 下一个尝试号（乱序/串线/缺失事件） |
| `ATTEMPT_ALREADY_STARTED` | 保留：同次尝试重复开始 |
| `STALE_TIMESTAMP` | eventTime 早于该位置已接受的时间（时间戳回退） |
| `MISSING_FAILURE_REASON` | FAILED 未提供 failureReason |

拒绝结果带 `message` 字段给出人类可解释的判定依据。被拒绝的事件**不会写入状态、不计指标、
不落盘**；真实接入端可修正/补齐后重投。

---

## 2. SLA 与重试规则

### 2.1 截止时间

```
deadline = scheduledAt + timeoutMillis
```

只按**调度周期**计算，与开始时间、重试次数无关。默认 `timeoutMillis=3600000`（60 分钟），
`maxAttempts=3`，可按流水线在 `SlaPolicyRegistry` 注册独立策略。

### 2.2 新鲜度判定 `SlaStatus`

| 结论 | 条件 | timedOut | recovered |
|---|---|---|---|
| `ON_TRACK` | `now ≤ deadline` 且尚未成功（运行中/等待重试/甚至已终态失败但尚未超时） | false | false |
| `ON_TIME` | 成功时间 **≤ deadline**（边界含等号） | false | false |
| `TIMEOUT` | `now > deadline` 仍未成功（终态失败、可重试失败后仍在跑/等待、超时未完成） | true | false |
| `LATE_SUCCESS` | 成功时间 **> deadline**：违约事实不可逆，迟到成功只从 TIMEOUT 收敛为此状态 | true | true |

**关键不变量：SLA 判定是已接受事件的纯函数，评估动作不写任何状态。**

- 短暂抖动/重复评估不会重复计数，结论只在底层事件变化时才可能变化；
- **超时一旦事实发生不可逆**：deadline 后才到的 `SUCCEEDED` 只能得到 `LATE_SUCCESS`，
  绝不会翻回 `ON_TIME`，因此不会产生错误的“恢复”结论；
- 终态失败且过截止时间 = TIMEOUT 且 `recovered=false`；
- 每个 `SlaVerdict` 都带 `basis` 文本，写明 scheduledAt/deadline/successAt/超出时长与状态，
  判定依据可直接展示。

### 2.3 失败与重试链路

- `FAILED(retryable=true)` 且当前 attempt < maxAttempts → `AWAITING_RETRY`，等待 attempt+1
  的 `STARTED`；
- `FAILED(retryable=false)`，或可重试但已达 `maxAttempts` → `FAILED_FINAL`；
- 上一次尝试没有 FAILED/SUCCEEDED 结束前，下一次 STARTED 被拒绝（防止乱序重试吞掉前一次
  失败事实——这是聚合“可重试失败计数”在乱序下保持正确的关键）。

### 2.4 指标聚合 `AggregationResult`

窗口按 `scheduledAt ∈ [from,to]` 归属（调度周期口径）：

- 运行数、成功数、终态失败数、超时数、迟到成功数、可重试失败数；
- 总尝试次数与每运行平均尝试次数；
- 运行总耗时/平均耗时（`finishedAt - firstStartedAt`，仅已结束运行计入）；
- 吞吐：每个已结束尝试 `itemCount / 尝试耗时`，汇总为 items/sec；
- SLA 计数直接复用单运行判定的纯逻辑，单查与聚合口径完全一致。

**顺序无关性**：所有指标都是对“已接受事件集合”的求和/计数（可交换、可结合），
事件到达顺序不同、或先重放快照再聚合，结果相同。

---

## 3. 状态一致性保证

1. **单锁串行归约**：事件的校验、状态变更、去重登记、快照落盘都在 `EventStore` 的同一把
   内置锁内完成。多线程并发提交按抢锁得到的总顺序串行处理，最终状态等于“某一种合法串行
   顺序”的归约结果，确定且唯一（见 `IngestConcurrencyTest`：同一批事件 16 线程 ×10 份
   并发，接受数/尝试数/终态精确不变）。
2. **幂等去重**：`eventId → runId` 全局索引 + 每运行已接受事件 ID 列表；重复提交返回
   `DUPLICATE_EVENT`，不改状态、不计数、不落盘。
3. **乱序不吞事实**：attempt 序号对齐、上尝试先结束、时间戳不回退三重检查；迟到的完成
   事件要么按原因拒绝，要么（合法迟到成功）得到稳定的 `LATE_SUCCESS`。
4. **本地快照与恢复**：已接受事件以 append-only JSONL 写入
   `pipeline.snapshot.path`（默认 `data/events.jsonl`）；只有落盘成功才更新内存。
   启动后（`ApplicationReadyEvent`）按文件中的接受顺序重放重建全部内存状态与去重索引，
   重放幂等且只执行一次。恢复后重放旧事件全部识别为重复，快照文件不增长。
5. **只读一致快照**：查询/聚合在锁内拷贝 `RunRecord` 后映射为不可变视图，遍历期间不受
   并发写入影响。

---

## 4. HTTP API

| 方法与路径 | 说明 |
|---|---|
| `POST /api/events` | 提交事件。接受 200；重复 200 且 `duplicate=true`；非法流转 422 + reason/message |
| `GET /api/runs/{runId}` | 运行状态只读视图（含每次尝试的开始/结束/耗时/条目数） |
| `GET /api/runs/{runId}/sla` | SLA 判定（status、deadline、delay、timedOut、recovered、basis） |
| `GET /api/pipelines/{name}/aggregation?from=&to=` | 时间窗聚合指标（含 basis） |

示例：

```bash
curl -s localhost:$PORT/api/events -H 'Content-Type: application/json' -d '{
  "eventId":"e1","runId":"r1","pipelineName":"demo","type":"SCHEDULED",
  "attempt":0,"eventTime":1759000000000,"retryable":null,"failureReason":null,"itemCount":0}'
```

---

## 5. 本地验证方式

要求 JDK 21、Maven（Spring Boot 3.4.1）。

```bash
# 全量测试（21 个，覆盖并发重复、乱序、SLA 边界、快照恢复、HTTP 端到端）
mvn test

# 启动服务（随机端口，见启动日志 "Tomcat started on port ..."）
mvn spring-boot:run

# 查看测试打印的“输入事件 => 判定依据”
grep -A2 '判定依据' target/surefire-reports/*output*.txt 2>/dev/null | head
```

测试与场景对应：

| 测试类 | 覆盖点 |
|---|---|
| `IngestConcurrencyTest` | 16/20/24 线程并发重复提交（恰好接受一次）、乱序并发按原因归类且状态自洽、50 运行并发重投后终态一致 |
| `IngestStateMachineTest` | 9 个场景：首事件、重复、回退、attempt 不匹配、终态拒绝、时间戳回退、缺失败原因、重试链与重试耗尽；断言均打印输入事件与依据 |
| `SlaBoundaryTest` | 成功恰在 deadline（ON_TIME）、超 1ms（LATE_SUCCESS）、超时后迟到成功不回退、重复评估幂等、终态失败前后变化、等待重试超时后恢复 |
| `MetricsAndRecoveryTest` | 正序 vs 逆序重投聚合完全相等；重建服务从 JSONL 快照恢复后运行视图、聚合一致，旧事件全部幂等、快照不增长 |
| `HttpApiIntegrationTest` | 真实容器端到端：200/422 状态码、重复标记、查询/SLA/聚合接口与 basis 输出 |

测试时钟使用可手动推进的 `MutableClock`，确定性构造抖动与迟到场景；快照文件落在
`target/test-data/` 与 `@TempDir`，不污染工作区（`data/` 已加入 `.gitignore`）。

## 6. 关键代码位置

```
model/      事件、运行状态、拒绝原因、SLA 判定、聚合结果（全部带可解释字段）
time/       Clock 抽象（生产 SystemClock，测试 MutableClock）
store/      EventStore：单锁串行化、eventId 去重、JSONL 快照与重放恢复
service/    IngestService 状态机 / SlaService 纯判定 / MetricsService 聚合 /
            RunQueryService 只读视图 / SlaPolicyRegistry 策略
controller/ REST 接口与统一异常 JSON
```
