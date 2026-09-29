package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.EventRejectedException;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.EventType;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RejectReason;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.AttemptState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.stereotype.Service;

/**
 * 事件接入服务。
 *
 * <p>拒绝策略(以 {@link RejectReason} 区分, 绝不静默覆盖):
 * <ol>
 *   <li>载荷不完整/非法 -> INVALID_PAYLOAD;</li>
 *   <li>全局 eventId 已存在且载荷一致 -> 幂等返回 duplicate=true; 载荷不一致 -> EVENT_ID_CONFLICT;</li>
 *   <li>非 RUN_STARTED 事件指向不存在的 run -> UNKNOWN_RUN;</li>
 *   <li>RUN_STARTED 重复启动同一 runId -> DUPLICATE_RUN;</li>
 *   <li>seq 小于已接收最大序号 -> STALE_SEQUENCE(状态回退/旧事件);</li>
 *   <li>seq 出现缺口(中间序号未到) -> OUT_OF_ORDER_GAP, 事件进入 pending 暂存, 缺口补齐后自动重放;</li>
 *   <li>生命周期回退/越权 -> ILLEGAL_TRANSITION; attempt 引用非法 -> UNKNOWN_ATTEMPT /
 *       ATTEMPT_ALREADY_FINISHED; 业务时间倒退 -> TIMESTAMP_REGRESSION。</li>
 * </ol>
 *
 * <p>一致性: 全部处理在 {@link RunStore} 的全局锁内完成; 同一 run 的并发 RUN_STARTED
 * 只有一个成功, 其余被拒; 最终状态只取决于被接受事件的集合, 与线程调度无关。
 */
@Service
public class EventIngestionService {

    private final RunStore store;
    private final SlaService slaService;

    public EventIngestionService(RunStore store, SlaService slaService) {
        this.store = store;
        this.slaService = slaService;
    }

    /** 接入单个事件。 */
    public IngestResult ingest(PipelineEvent event) {
        Objects.requireNonNull(event, "event");
        validateShape(event);
        return store.withLock(() -> doIngest(event));
    }

    /** 批量接入; 单个事件被拒绝不会影响其余事件, 返回逐条结果(拒绝以 accepted=false 表达)。 */
    public List<IngestResult> ingestBatch(List<PipelineEvent> events) {
        Objects.requireNonNull(events, "events");
        return store.withLock(() -> {
            List<IngestResult> results = new ArrayList<>(events.size());
            for (PipelineEvent event : events) {
                try {
                    validateShape(event);
                    results.add(doIngest(event));
                } catch (EventRejectedException ex) {
                    results.add(new IngestResult(false,
                            event == null ? null : event.eventId(),
                            event == null ? null : event.runId(), false, ex.getReason().name()));
                }
            }
            return results;
        });
    }

    /** 校验字段形状(无状态部分)。必须在调用前(锁外即可)执行。 */
    private void validateShape(PipelineEvent e) {
        reject(e.eventId() == null || e.eventId().isBlank(), e, RejectReason.INVALID_PAYLOAD,
                "eventId 缺失");
        reject(e.pipelineId() == null || e.pipelineId().isBlank(), e, RejectReason.INVALID_PAYLOAD,
                "pipelineId 缺失");
        reject(e.runId() == null || e.runId().isBlank(), e, RejectReason.INVALID_PAYLOAD,
                "runId 缺失");
        reject(e.type() == null, e, RejectReason.INVALID_PAYLOAD, "type 缺失");
        reject(e.occurredAt() == null, e, RejectReason.INVALID_PAYLOAD, "occurredAt 缺失");
        reject(e.seq() == null || e.seq() <= 0, e, RejectReason.INVALID_PAYLOAD,
                "seq 缺失或非正数");
        if (e.type() == EventType.RUN_STARTED) {
            reject(e.scheduledTime() == null, e, RejectReason.INVALID_PAYLOAD,
                    "RUN_STARTED 必须携带 scheduledTime");
            reject(e.slaMillis() == null || e.slaMillis() < 0, e, RejectReason.INVALID_PAYLOAD,
                    "RUN_STARTED 必须携带非负 slaMillis");
        }
        if (e.type() == EventType.ATTEMPT_STARTED
                || e.type() == EventType.ATTEMPT_RETRIABLE_FAILURE
                || e.type() == EventType.ATTEMPT_SUCCEEDED) {
            reject(e.attemptSeq() == null || e.attemptSeq() <= 0, e,
                    RejectReason.INVALID_PAYLOAD, "ATTEMPT_* 事件必须携带正数 attemptSeq");
        }
    }

    /** 真正的接入逻辑; 必须在 store 锁内执行。 */
    private IngestResult doIngest(PipelineEvent event) {
        // 1) 全局幂等
        String ownerRunId = store.globalEventIdRun(event.eventId());
        if (ownerRunId != null) {
            RunState owner = store.findRun(ownerRunId);
            if (isSameEventPayload(owner, event)) {
                return IngestResult.duplicate(event.eventId(), ownerRunId);
            }
            throw rejected(event, RejectReason.EVENT_ID_CONFLICT,
                    "eventId 已被不同内容的事件占用");
        }

        // 2) RUN_STARTED 建 run; 其余事件要求 run 存在
        if (event.type() == EventType.RUN_STARTED) {
            if (store.findRun(event.runId()) != null) {
                throw rejected(event, RejectReason.DUPLICATE_RUN,
                        "runId 已启动, 重复的 RUN_STARTED 被拒绝");
            }
            RunState run = newRunState(event);
            store.createRun(run);
            accept(run, event);
            store.registerGlobalEventId(event.eventId(), run.runId);
            // 启动时也推进一次 SLA(通常为 PENDING), 随后统一持久化
            slaService.evaluateLocked(run, event.occurredAt());
            store.persistLocked();
            drainPending(run);
            return IngestResult.accepted(event.eventId(), run.runId);
        }

        RunState run = store.findRun(event.runId());
        if (run == null) {
            throw rejected(event, RejectReason.UNKNOWN_RUN,
                    "run 不存在, 尚未收到 RUN_STARTED");
        }

        // 3) 乱序/回退: 以 seq 判定
        long expected = run.maxSeq + 1;
        if (event.seq() < expected) {
            throw rejected(event, RejectReason.STALE_SEQUENCE,
                    "seq=" + event.seq() + " 已被越过(当前 maxSeq=" + run.maxSeq + ")");
        }
        if (event.seq() > expected) {
            // 缺口: 暂存(同一 seq 只保留首条, 其余等流转校验时暴露), 不覆盖任何状态
            run.pending.add(event);
            throw rejected(event, RejectReason.OUT_OF_ORDER_GAP,
                    "期望 seq=" + expected + ", 实际 seq=" + event.seq()
                            + ", 事件暂存等待缺口补齐");
        }

        // 4) 业务时间不得倒退
        if (run.lastEventAt != null && event.occurredAt().isBefore(run.lastEventAt)) {
            throw rejected(event, RejectReason.TIMESTAMP_REGRESSION,
                    "occurredAt=" + event.occurredAt() + " 早于上一事件 " + run.lastEventAt);
        }

        // 5) 状态机流转
        applyTransition(run, event);

        accept(run, event);
        store.registerGlobalEventId(event.eventId(), run.runId);
        store.persistLocked();
        drainPending(run);
        return IngestResult.accepted(event.eventId(), run.runId);
    }

    /** 缺口补齐后, 按 seq 顺序重放暂存事件; 重放被拒的事件保留在 pending 中等待后续补齐。 */
    private void drainPending(RunState run) {
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            long expected = run.maxSeq + 1;
            PipelineEvent next = null;
            for (PipelineEvent e : run.pending) {
                if (e.seq() == expected) {
                    next = e;
                    break;
                }
            }
            if (next == null) {
                return;
            }
            run.pending.remove(next);
            // 重放走完整校验; 若仍非法(如时间倒退/非法流转), 该事件丢弃出队并记录, 不再阻塞后续
            try {
                if (run.lastEventAt != null && next.occurredAt().isBefore(run.lastEventAt)) {
                    throw rejected(next, RejectReason.TIMESTAMP_REGRESSION,
                            "重放时发现 occurredAt 倒退");
                }
                applyTransition(run, next);
                accept(run, next);
                store.registerGlobalEventId(next.eventId(), run.runId);
                progressed = true;
            } catch (EventRejectedException ex) {
                // 乱序补齐后仍非法的事件: 已按原因拒绝, 不再回队, 避免毒丸永久阻塞缺口
            }
        }
    }

    private void accept(RunState run, PipelineEvent event) {
        run.maxSeq = Math.max(run.maxSeq, event.seq());
        run.lastEventAt = event.occurredAt();
        run.acceptedEventIds.put(event.eventId(), event.seq());
    }

    private RunState newRunState(PipelineEvent e) {
        RunState run = new RunState();
        run.pipelineId = e.pipelineId();
        run.runId = e.runId();
        run.status = RunStatus.RUNNING;
        run.scheduledTime = e.scheduledTime();
        run.startedAt = e.occurredAt();
        run.slaMillis = e.slaMillis();
        run.maxSeq = 0;
        run.lastEventAt = e.occurredAt();
        return run;
    }

    /** 校验并应用一次合法的生命周期流转。任何回退/越权都按原因拒绝。 */
    private void applyTransition(RunState run, PipelineEvent e) {
        EventType type = e.type();

        if (run.status != RunStatus.RUNNING) {
            throw rejected(e, RejectReason.ILLEGAL_TRANSITION,
                    "run 已处于终态 " + run.status + ", 拒绝事件 " + type);
        }

        switch (type) {
            case RUN_STARTED -> throw rejected(e, RejectReason.DUPLICATE_RUN,
                    "run 已启动, 流转中再次出现 RUN_STARTED");

            case ATTEMPT_STARTED -> {
                // attemptSeq 必须严格接续已有尝试
                long nextAttemptSeq = (long) run.attempts.size() + 1;
                if (e.attemptSeq() != nextAttemptSeq) {
                    throw rejected(e, RejectReason.ILLEGAL_TRANSITION,
                            "ATTEMPT_STARTED 的 attemptSeq 应为 " + nextAttemptSeq
                                    + ", 实际 " + e.attemptSeq());
                }
                AttemptState attempt = new AttemptState();
                attempt.attemptSeq = e.attemptSeq();
                attempt.startedAt = e.occurredAt();
                attempt.processedCount = e.processedCount() == null ? 0 : e.processedCount();
                run.attempts.put(attempt.attemptSeq, attempt);
            }

            case ATTEMPT_RETRIABLE_FAILURE, ATTEMPT_SUCCEEDED -> {
                AttemptState attempt = run.attempts.get(e.attemptSeq());
                if (attempt == null) {
                    throw rejected(e, RejectReason.UNKNOWN_ATTEMPT,
                            "attemptSeq=" + e.attemptSeq() + " 从未 STARTED");
                }
                if (attempt.outcome != null) {
                    throw rejected(e, RejectReason.ATTEMPT_ALREADY_FINISHED,
                            "attemptSeq=" + e.attemptSeq() + " 已结束(" + attempt.outcome + ")");
                }
                attempt.finishedAt = e.occurredAt();
                attempt.outcome = type == EventType.ATTEMPT_SUCCEEDED
                        ? com.github.highcumontoa.pipelineslaobservabilityjava.domain.AttemptOutcome.SUCCEEDED
                        : com.github.highcumontoa.pipelineslaobservabilityjava.domain.AttemptOutcome.RETRIABLE_FAILURE;
                attempt.errorCode = e.errorCode();
                if (e.processedCount() != null) {
                    attempt.processedCount = e.processedCount();
                }
            }

            case RUN_SUCCEEDED -> {
                AttemptState last = lastAttempt(run);
                if (last == null || last.outcome
                        != com.github.highcumontoa.pipelineslaobservabilityjava.domain.AttemptOutcome.SUCCEEDED) {
                    throw rejected(e, RejectReason.ILLEGAL_TRANSITION,
                            "RUN_SUCCEEDED 之前必须有一次 SUCCEEDED 的 attempt");
                }
                finishRun(run, e, RunStatus.SUCCEEDED);
            }

            case RUN_FAILED -> {
                AttemptState last = lastAttempt(run);
                if (last == null || last.outcome
                        != com.github.highcumontoa.pipelineslaobservabilityjava.domain.AttemptOutcome.RETRIABLE_FAILURE) {
                    throw rejected(e, RejectReason.ILLEGAL_TRANSITION,
                            "终态 RUN_FAILED 必须紧跟一次 RETRIABLE_FAILURE(重试耗尽)");
                }
                run.terminalErrorCode = e.errorCode() != null ? e.errorCode() : last.errorCode;
                finishRun(run, e, RunStatus.FAILED);
            }
        }
    }

    private void finishRun(RunState run, PipelineEvent e, RunStatus status) {
        run.status = status;
        run.finishedAt = e.occurredAt();
        // 终态事件携带的计数并入最后一次尝试
        if (e.processedCount() != null) {
            AttemptState last = lastAttempt(run);
            if (last != null) {
                last.processedCount = e.processedCount();
            }
        }
        // 以终态业务时间做一次性 SLA 判定(开启/关闭超时事件)
        slaService.evaluateLocked(run, e.occurredAt());
    }

    private AttemptState lastAttempt(RunState run) {
        AttemptState last = null;
        for (AttemptState a : run.attempts.values()) {
            last = a;
        }
        return last;
    }

    /** 判断重复 eventId 的载荷是否与首次一致(幂等)。 */
    private boolean isSameEventPayload(RunState owner, PipelineEvent e) {
        if (owner == null) {
            return false;
        }
        Long firstSeq = owner.acceptedEventIds.get(e.eventId());
        return firstSeq != null && firstSeq.equals(e.seq())
                && owner.runId.equals(e.runId())
                && owner.pipelineId.equals(e.pipelineId());
    }

    private boolean reject(boolean condition, PipelineEvent e, RejectReason reason, String detail) {
        if (condition) {
            throw rejected(e, reason, detail);
        }
        return false;
    }

    private EventRejectedException rejected(PipelineEvent e, RejectReason reason, String detail) {
        String runId = e == null ? null : e.runId();
        String eventId = e == null ? null : e.eventId();
        return new EventRejectedException(reason, runId, eventId, detail);
    }

    /** 仅供测试/诊断: 列出 run 中暂存的乱序事件数。 */
    public int pendingCount(String runId) {
        return store.withLock(() -> {
            RunState run = store.findRun(runId);
            return run == null ? -1 : run.pending.size();
        });
    }
}
