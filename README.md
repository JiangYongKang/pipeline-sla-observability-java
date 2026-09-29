# Pipeline SLA Observability Service

流水线运行状态接入与 SLA 监控服务（Spring Boot 3 / Java 21）。
使用**本地内存 + JSON 快照文件**保存运行事件应用后的状态，提供：

- 流水线运行事件接入与状态流转（重复/乱序/回退事件按原因拒绝，不静默覆盖）；
- 新鲜度延迟与 SLA 超时/恢复判定（以业务时间为准，迟到/抖动不产生重复计数）；
- 失败与重试链路、每轮尝试的次数/耗时/吞吐及流水线级聚合；
- 多线程并发接入下的最终状态一致，以及从本地快照恢复后结论稳定。

---

## 1. 事件模型

接入侧提交统一的 `PipelineEvent`（JSON）：

| 字段 | 含义 |
| --- | --- |
| `eventId` | 事件全局唯一 ID，用于**幂等去重**与冲突识别 |
| `pipelineId` / `runId` | 流水线 / 一次调度运行 |
| `seq` | run 内严格单调递增的事件序号（从 1 开始），用于识别乱序 |
| `type` | 事件类型（见下） |
| `occurredAt` | 事件**业务发生时间**（判定一律以它为准，而非服务端接收时刻） |
| `scheduledTime` | 计划开始时间（`RUN_STARTED` 必填） |
| `slaMillis` | SLA 时长（毫秒，`RUN_STARTED` 必填） |
| `attemptSeq` | 尝试序号（`ATTEMPT_*` 事件必填，从 1 开始） |
| `processedCount` | 累计处理条数（可空，用于吞吐聚合） |
| `errorCode` | 失败错误码（失败类事件可空） |

合法生命周期：

```
RUN_STARTED
  └─ ( ATTEMPT_STARTED → (ATTEMPT_RETRIABLE_FAILURE | ATTEMPT_SUCCEEDED) )*   // 可重复多轮
      └─ RUN_SUCCEEDED     // 前置：最后一次 attempt 为 SUCCEEDED
         或 RUN_FAILED     // 前置：最后一次 attempt 为 RETRIABLE_FAILURE（重试耗尽）
```

`RUN_SUCCEEDED` / `RUN_FAILED` 之后 run 进入终态，不再接受任何事件。

### 拒绝原因（`RejectReason`，不静默接受/覆盖）

| 原因 | 触发条件 |
| --- | --- |
| `INVALID_PAYLOAD` | 必填字段缺失/非法、未知类型 |
| `DUPLICATE_EVENT` / `EVENT_ID_CONFLICT` | 同 `eventId` 已存在：载荷一致→幂等成功（`duplicate=true`）；载荷不一致→冲突拒绝 |
| `UNKNOWN_RUN` | 非启动事件指向从未 `RUN_STARTED` 的 run |
| `DUPLICATE_RUN` | 同一 `runId` 重复 `RUN_STARTED` |
| `STALE_SEQUENCE` | `seq` 小于已接收最大序号（旧事件/状态回退） |
| `OUT_OF_ORDER_GAP` | `seq` 出现缺口；事件进入 `pending` 暂存，缺口补齐后按序自动重放 |
| `ILLEGAL_TRANSITION` | 生命周期回退/越权、终态后再来事件、attempt 编号不接续 |
| `UNKNOWN_ATTEMPT` / `ATTEMPT_ALREADY_FINISHED` | 引用未开始的尝试 / 尝试已结束再次上报结局 |
| `TIMESTAMP_REGRESSION` | `occurredAt` 早于该 run 已接收事件的时间 |

乱序语义：乱序事件**不会被丢弃或覆盖**，而是因 `OUT_OF_ORDER_GAP` 暂存；当缺口事件
到达时自动按 `seq` 重放。若重放时事件仍非法（例如时间倒退），按对应原因拒绝且不污染状态。

---

## 2. SLA 与新鲜度规则

所有判定只依赖事件携带的**业务时间**，与事件到达服务的时间无关，因此迟到、短暂抖动
不会改变结论。

- 新鲜度延迟：`freshnessDelay = startedAt - scheduledTime`（正数表示晚启动）。
- 截止时间：`deadline = scheduledTime + slaMillis`（`scheduledTime` 缺失时以 `startedAt` 为基准）。
- 终态判定：
  - `finishedAt <= deadline` → `MET`（恰在 deadline 上也算未超时）；
  - `finishedAt > deadline` → `BREACHED`（晚 1ms 也算超时）。
- 进行中判定：观察时刻（周期扫描用当前时钟）`> deadline` → `OPEN`，否则 `PENDING`。

### 超时事件（incident）的计数依据——为什么不会重复计数

- 每个 run 的超时 incident **最多开启一次、关闭一次**（以 runId 为键唯一保存）；
- 进行中的 run 在周期扫描中越过 deadline：开启 incident（`OPEN`）；重复扫描只返回
  既有 incident，不二次计数；
- 迟到的完成事件：
  - 若业务上 `finishedAt <= deadline`（完成事件只是网络迟到）→ SLA 为 `MET`，
    incident 关闭并标记为 **recovered**，不计超时成立；
  - 若 `finishedAt > deadline` → `BREACHED`，incident 关闭并标记为 **breached**；
- 若终态事件本身到达很晚（此前从未扫描过），也只会补开并立即关闭**一次** incident。

聚合中 `timeoutIncidentsOpened / Breached / Recovered` 三个计数均以落盘的 incident
为唯一口径，互不重叠，因此抖动或重复上报不会造成重复计数。

---

## 3. 重试与指标

- `ATTEMPT_RETRIABLE_FAILURE` 表示**可重试失败**：之后允许下一个 `ATTEMPT_STARTED`；
- `RUN_FAILED` 表示**终态失败**（重试耗尽），其前置必须是一次可重试失败；
- 每次尝试记录：开始/结束时间、结局、错误码、处理条数；
  `attemptDuration = finishedAt - startedAt`；
- 每个运行：`attemptCount`、`retriableFailureCount`、`runDuration = finishedAt - startedAt`、
  `processedCount = Σ attempt.processedCount`、
  `throughputPerSec = processedCount * 1000 / runDuration`；
- 流水线聚合：按状态/SLA 分类计数、尝试与可重试失败总数、超时 incident 三类计数、
  总处理量、总运行时长与聚合吞吐。

**顺序无关性**：聚合只读取“已接受事件形成的最终状态”。不同 run 的事件无论分组到达
还是交错到达，最终每个 run 的状态、SLA 与聚合结果完全一致（由 `RetryAndMetricsTest`
的两个“世界”对照验证）。

---

## 4. 状态一致性保证

- `RunStore` 使用一把全局可重入锁串行化**所有**状态变更与快照写入；
- 同一 run 的并发 `RUN_STARTED`：只有一个成功，其余得到 `DUPLICATE_RUN`；
- 同一事件的并发重复提交：恰好一个被接受，其余全部幂等重复（`accepted/duplicate`
  计数见 `ConcurrentIngestionTest`：16 线程 × 4 事件 → 4 accepted / 60 duplicate / 0 rejected）；
- 事件先做全局 `eventId` 幂等，再做 `seq` 乱序/回退校验，最后做状态机流转校验；
- 快照采用“写临时文件 + 原子移动”，在锁内序列化，避免崩溃留下半文件或读到撕裂内容。

### 快照恢复

快照保存的是事件应用后的**最终状态**（runs + incidents），而不是原始事件流。
重启后：运行状态、SLA 结论、attempt/指标聚合、已接受 `eventId`（幂等/冲突识别）
全部恢复，查询结果与重启前一致。配置项：

```properties
# 留空=纯内存；设置路径则每次变更后原子写入并在启动时恢复
pipeline.snapshot.path=${PIPELINE_SNAPSHOT_PATH:}
```

---

## 5. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/events` | 接入单个事件；成功 200（`duplicate=true` 表示幂等重复）；非法 409，body 含结构化 `reason` |
| `POST /api/events/batch` | 批量接入，逐条返回结果；单条非法不影响其余 |
| `GET  /api/runs/{runId}` | 单运行详情（状态、SLA 及 `slaReason` 判定依据、attempt 链路） |
| `GET  /api/runs` | 全部运行 |
| `GET  /api/pipelines/{pipelineId}/metrics` | 流水线聚合指标 |
| `POST /api/sla/sweep` | 以当前时钟推进 SLA 扫描（模拟调度周期到来），返回 `openTimeoutRuns` |

示例：

```bash
curl -s -XPOST localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "eventId":"e1","pipelineId":"p","runId":"r1","seq":1,
  "type":"RUN_STARTED","occurredAt":"2026-09-29T00:00:00Z",
  "scheduledTime":"2026-09-29T00:00:00Z","slaMillis":60000
}'
# 非法事件示例响应：HTTP 409
# {"accepted":false,"reason":"DUPLICATE_RUN","eventId":"e9","runId":"r1","message":"..."}
```

---

## 6. 本地验证

```bash
# 编译 + 全部单测（23 个用例，覆盖并发重复、乱序、SLA 边界、重试、快照恢复、HTTP）
mvn test

# 仅运行某一组
mvn -Dtest=SlaBoundaryTest test

# 指定快照路径启动（验证持久化/恢复）
PIPELINE_SNAPSHOT_PATH=/tmp/pipeline-snapshot.json mvn spring-boot:run
```

测试清单：

- `EventIngestionServiceTest`：正常流转、幂等/冲突、重复启动、未知 run、
  非法流转、attempt 重复结束、乱序缺口与重放、时间倒退；
- `ConcurrentIngestionTest`：16 线程并发重复提交的最终一致性 + 快照可恢复；
- `SlaBoundaryTest`：deadline 恰好/晚 1ms、新鲜度延迟、超时只开一次、
  迟到完成事件的恢复/超时结论；
- `RetryAndMetricsTest`：可重试失败 vs 终态失败、尝试次数/耗时/吞吐、
  分组到达 vs 交错到达聚合一致、批量隔离；
- `SnapshotRecoveryTest`：重启前后状态/指标一致、旧 eventId 仍幂等；
- `WebApiTest`：200/409 结构化原因、查询、聚合、sweep。

测试断言中通过 `System.out.println` 打印输入事件与判定依据（SLA 结论附带
`slaReason` 文字说明），便于复现与解释。
