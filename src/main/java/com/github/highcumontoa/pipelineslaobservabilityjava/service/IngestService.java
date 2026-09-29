package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.Attempt;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.EventType;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RejectReason;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunRecord;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaPolicy;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 事件接入与运行状态机。
 *
 * <p>规则（任何不满足的事件都按 {@link RejectReason} 显式拒绝，绝不静默接受/覆盖）：
 * <ol>
 *   <li>eventId 全局唯一：重复提交识别为 DUPLICATE_EVENT，幂等返回当前状态；</li>
 *   <li>每个 runId 的首个事件必须是 SCHEDULED，否则 UNKNOWN_RUN /
 *       FIRST_EVENT_MUST_BE_SCHEDULED；</li>
 *   <li>终态（SUCCEEDED / FAILED_FINAL）之后的任何事件拒绝为 TERMINAL_RUN；</li>
 *   <li>STARTED 的 attempt 必须严格等于“下一个尝试号”；
 *       SUCCEEDED/FAILED 的 attempt 必须等于“当前进行中的尝试号”，
 *       否则 ATTEMPT_MISMATCH（覆盖乱序到达、串线、缺失开始事件）；</li>
 *   <li>STARTED 已开始的尝试再次 STARTED 拒绝为 ATTEMPT_ALREADY_STARTED；</li>
 *   <li>事件时间戳回退（早于该位置已接受时间）拒绝为 STALE_TIMESTAMP；</li>
 *   <li>FAILED 必须带 failureReason，否则 MISSING_FAILURE_REASON；</li>
 *   <li>超过策略最大尝试次数仍尝试 STARTED 拒绝为 ILLEGAL_TRANSITION。</li>
 * </ol>
 *
 * <p>一致性：校验、状态变更、去重登记与持久化在 {@link EventStore} 的同一把锁内完成，
 * 多线程并发提交按锁竞争得到的总顺序串行归约，最终状态确定且唯一。
 */
@Service
public class IngestService implements EventStore.EventReplayer {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final EventStore store;
    private final SlaPolicyRegistry policyRegistry;

    public IngestService(EventStore store, SlaPolicyRegistry policyRegistry) {
        this.store = store;
        this.policyRegistry = policyRegistry;
    }

    /** 容器就绪后触发快照重放（此时本 Bean 已构造完成，重放器可用）。 */
    @EventListener(ApplicationReadyEvent.class)
    public void recover() {
        store.recoverIfNeeded(this);
    }

    @Override
    public void replay(PipelineEvent event) {
        // 快照中的事件都曾被接受：无校验、无重复拒绝、不再持久化，仅重建内存状态。
        applyEvent(event, false);
    }

    /**
     * 接入一个事件。返回结果永远非 null；accepted=false 时 reason 标明拒绝原因。
     */
    public IngestResult ingest(PipelineEvent event) {
        String invalid = validateShape(event);
        if (invalid != null) {
            return IngestResults.reject(RejectReason.ILLEGAL_TRANSITION, invalid, RunState.SCHEDULED);
        }
        synchronized (store) {
            if (store.isKnownEvent(event.eventId())) {
                RunRecord rec = store.getRecord(event.runId());
                RunState st = rec != null ? rec.getState() : null;
                boolean sameRun = event.runId().equals(store.runOfEvent(event.eventId()));
                String msg = sameRun
                        ? "事件已接受过（幂等重复提交），不重复计数、不覆盖状态，runId=" + event.runId()
                        : "eventId 已属于另一个 runId，拒绝串线重复，eventId=" + event.eventId();
                return new IngestResult(false, true, RejectReason.DUPLICATE_EVENT, st, msg);
            }
            return applyEvent(event, true);
        }
    }

    private IngestResult applyEvent(PipelineEvent event, boolean persist) {
        RunRecord rec = store.getRecord(event.runId());

        if (rec == null) {
            if (event.type() == EventType.SCHEDULED) {
                rec = new RunRecord();
                rec.setRunId(event.runId());
                rec.setPipelineName(event.pipelineName());
                rec.setScheduledAt(event.eventTime());
                rec.setState(RunState.SCHEDULED);
                rec.getAcceptedEventIds().add(event.eventId());
                if (persist) {
                    store.append(event, rec);
                } else {
                    store.putRecord(rec);
                    store.indexAcceptedEvent(event);
                }
                return accept(rec, "SCHEDULED 已接受，调度时间=" + event.eventTime());
            }
            return IngestResults.reject(RejectReason.UNKNOWN_RUN,
                    "运行从未 SCHEDULED，拒绝 " + event.type() + ": runId=" + event.runId(),
                    RunState.SCHEDULED);
        }

        RunState state = rec.getState();
        if (state == RunState.SUCCEEDED || state == RunState.FAILED_FINAL) {
            return reject(rec, RejectReason.TERMINAL_RUN,
                    "运行已处于终态 " + state + "，拒绝事件 " + event.type()
                            + "（含重复 SCHEDULED/回退事件）");
        }

        if (event.type() == EventType.SCHEDULED) {
            return reject(rec, RejectReason.ILLEGAL_TRANSITION,
                    "该运行已存在，SCHEDULED 不能再次出现（状态回退）: runId=" + event.runId());
        }

        if (event.attempt() < 1) {
            return reject(rec, RejectReason.ATTEMPT_MISMATCH,
                    "attempt 必须 >= 1，实际=" + event.attempt());
        }

        switch (event.type()) {
            case STARTED -> {
                int expected = rec.getAttempts().size() + 1;
                if (event.attempt() != expected) {
                    return reject(rec, RejectReason.ATTEMPT_MISMATCH,
                            "STARTED 的 attempt=" + event.attempt() + " 与重试链期望的 "
                                    + expected + " 不一致（乱序/缺失事件）");
                }
                if (!rec.getAttempts().isEmpty()) {
                    Attempt last = rec.getAttempts().get(rec.getAttempts().size() - 1);
                    if (last.getFinishedAt() == null) {
                        return reject(rec, RejectReason.ILLEGAL_TRANSITION,
                                "第 " + last.getAttempt() + " 次尝试尚未收到 FAILED/SUCCEEDED，"
                                        + "不能开始第 " + event.attempt() + " 次尝试（乱序重试）");
                    }
                }
                if (event.attempt() > policyRegistry.policyFor(rec.getPipelineName()).maxAttempts()) {
                    return reject(rec, RejectReason.ILLEGAL_TRANSITION,
                            "超过最大尝试次数 "
                                    + policyRegistry.policyFor(rec.getPipelineName()).maxAttempts()
                                    + "，拒绝第 " + event.attempt() + " 次 STARTED");
                }
                Long notBefore = rec.getAttempts().isEmpty()
                        ? rec.getScheduledAt()
                        : rec.getAttempts().get(rec.getAttempts().size() - 1).getStartedAt();
                if (notBefore != null && event.eventTime() < notBefore) {
                    return reject(rec, RejectReason.STALE_TIMESTAMP,
                            "STARTED 时间 " + event.eventTime() + " 早于已接受时间 " + notBefore
                                    + "（时间戳回退）");
                }
                Attempt a = new Attempt();
                a.setAttempt(event.attempt());
                a.setStartedAt(event.eventTime());
                a.setItemCount(event.itemCount() == null ? 0 : event.itemCount());
                rec.getAttempts().add(a);
                if (rec.getFirstStartedAt() == null) {
                    rec.setFirstStartedAt(event.eventTime());
                }
                rec.setState(RunState.RUNNING);
                commit(event, rec, persist);
                return accept(rec, "第 " + event.attempt() + " 次尝试 STARTED 已接受");
            }
            case SUCCEEDED, FAILED -> {
                int expected = rec.getAttempts().size();
                if (event.attempt() != expected || expected == 0) {
                    return reject(rec, RejectReason.ATTEMPT_MISMATCH,
                            "完成事件 " + event.type() + " 的 attempt=" + event.attempt()
                                    + "，但当前进行中的是第 " + expected + " 次尝试（乱序/缺失 STARTED）");
                }
                Attempt cur = rec.getAttempts().get(expected - 1);
                if (event.type() == EventType.FAILED
                        && (event.failureReason() == null || event.failureReason().isBlank())) {
                    return reject(rec, RejectReason.MISSING_FAILURE_REASON,
                            "FAILED 事件必须提供 failureReason");
                }
                if (event.eventTime() < cur.getStartedAt()) {
                    return reject(rec, RejectReason.STALE_TIMESTAMP,
                            "完成事件时间 " + event.eventTime() + " 早于本次尝试开始时间 "
                                    + cur.getStartedAt() + "（时间戳回退）");
                }
                cur.setFinishedAt(event.eventTime());
                if (event.itemCount() != null) {
                    cur.setItemCount(event.itemCount());
                }
                rec.setFinishedAt(event.eventTime());
                if (event.type() == EventType.SUCCEEDED) {
                    cur.setSucceeded(true);
                    rec.setState(RunState.SUCCEEDED);
                    commit(event, rec, persist);
                    return accept(rec, "第 " + event.attempt() + " 次尝试 SUCCEEDED，运行终态成功");
                }
                boolean retryable = Boolean.TRUE.equals(event.retryable());
                cur.setRetryableFailure(retryable);
                cur.setFailureReason(event.failureReason());
                SlaPolicy policy = policyRegistry.policyFor(rec.getPipelineName());
                boolean canRetry = retryable && event.attempt() < policy.maxAttempts();
                rec.setState(canRetry ? RunState.AWAITING_RETRY : RunState.FAILED_FINAL);
                commit(event, rec, persist);
                String why = canRetry
                        ? "可重试失败，等待第 " + (event.attempt() + 1) + " 次尝试"
                        : (!retryable ? "终态失败：失败被标记为不可重试"
                                : "终态失败：已达最大尝试次数 " + policy.maxAttempts());
                return accept(rec, "第 " + event.attempt() + " 次尝试 FAILED 已接受；" + why);
            }
            default -> {
                return reject(rec, RejectReason.ILLEGAL_TRANSITION,
                        "不支持的事件类型: " + event.type());
            }
        }
    }

    private void commit(PipelineEvent event, RunRecord rec, boolean persist) {
        rec.getAcceptedEventIds().add(event.eventId());
        if (persist) {
            store.append(event, rec);
        } else {
            store.indexAcceptedEvent(event);
        }
    }

    private static String validateShape(PipelineEvent e) {
        if (e == null) {
            return "事件体为空";
        }
        if (isBlank(e.eventId())) {
            return "eventId 不能为空";
        }
        if (isBlank(e.runId())) {
            return "runId 不能为空";
        }
        if (e.type() == null) {
            return "type 不能为空";
        }
        if (isBlank(e.pipelineName())) {
            return "pipelineName 不能为空";
        }
        return null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static IngestResult accept(RunRecord rec, String message) {
        return new IngestResult(true, false, null, rec.getState(), message);
    }

    private static IngestResult reject(RunRecord rec, RejectReason reason, String message) {
        return IngestResults.reject(reason, message, rec.getState());
    }

    /** 便于批量调用方观察：按 runId 列出已接受事件 ID（诊断用）。 */
    public List<String> acceptedEventIds(String runId) {
        RunRecord rec = store.getRecord(runId);
        return rec == null ? List.of() : List.copyOf(rec.getAcceptedEventIds());
    }
}
