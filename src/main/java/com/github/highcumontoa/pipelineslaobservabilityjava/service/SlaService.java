package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.SlaStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.TimeoutIncident;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.time.Clock;
import java.time.Instant;
import org.springframework.stereotype.Service;

/**
 * 新鲜度与 SLA 判定服务。
 *
 * <p>所有判定只使用事件业务时间({@code occurredAt}/{@code scheduledTime})与 deadline,
 * 与事件到达服务的时刻无关, 因此迟到、乱序或短暂抖动不会改变结论。
 *
 * <p>判定规则:
 * <ul>
 *   <li>新鲜度延迟 freshnessDelay = startedAt - scheduledTime(可正可负, 为正表示晚启动);</li>
 *   <li>deadline = scheduledTime + slaMillis(scheduledTime 缺失时退化为 startedTime + slaMillis);</li>
 *   <li>终态运行: finishedAt &lt;= deadline 为 {@link SlaStatus#MET}, 否则 {@link SlaStatus#BREACHED};</li>
 *   <li>进行中运行: 观察时刻 &gt; deadline 则开启超时事件({@link SlaStatus#OPEN}), 否则 {@link SlaStatus#PENDING};</li>
 *   <li>超时事件(incident)每个 run 最多开启一次; 运行结束时关闭一次:
 *       deadline 后结束记为 breached, deadline 前结束(迟到完成事件)记为 recovered,
 *       因而重复上报/抖动不会产生二次计数。</li>
 * </ul>
 */
@Service
public class SlaService {

    private final Clock clock;
    private final RunStore store;

    public SlaService(Clock clock, RunStore store) {
        this.clock = clock;
        this.store = store;
    }

    /** deadline = 计划时间 + SLA 时长; 计划时间缺失时以实际开始时间为基准。 */
    public Instant deadline(RunState run) {
        Instant base = run.scheduledTime != null ? run.scheduledTime : run.startedAt;
        if (base == null) {
            return null;
        }
        return base.plusMillis(run.slaMillis);
    }

    /** 新鲜度延迟(毫秒) = 实际开始时间 - 计划开始时间。计划时间缺失时记 0。 */
    public long freshnessDelayMillis(RunState run) {
        if (run.scheduledTime == null || run.startedAt == null) {
            return 0L;
        }
        return run.startedAt.toEpochMilli() - run.scheduledTime.toEpochMilli();
    }

    /**
     * 在指定观察时刻推进单个运行的 SLA 状态并按需落超时事件。
     * 必须在 {@link RunStore} 锁内调用。
     *
     * @param observedAt 观察时刻: 终态事件用其 occurredAt; 周期扫描用当前时钟
     */
    public SlaEvaluation evaluateLocked(RunState run, Instant observedAt) {
        Instant deadline = deadline(run);
        if (deadline == null) {
            return new SlaEvaluation(SlaStatus.PENDING,
                    "deadline 尚未确定(slaMillis 或基准时间缺失)", null);
        }

        TimeoutIncident incident = store.incidentFor(run.runId);

        if (run.status != RunStatus.RUNNING) {
            // 终态: 以 finishedAt 与 deadline 的关系做一次性结论
            boolean inTime = !run.finishedAt.isAfter(deadline);
            if (incident == null) {
                // 结束前从未观察到超时
                if (inTime) {
                    return new SlaEvaluation(SlaStatus.MET,
                            "运行于 " + run.finishedAt + " 结束, 不晚于 deadline " + deadline, null);
                }
                // 终态完成事件本身迟到(到达时已在 deadline 后): 只补开并立即关闭一次
                incident = openIncident(run, deadline, run.finishedAt);
                closeIncident(incident, run.finishedAt, false);
                return new SlaEvaluation(SlaStatus.BREACHED,
                        "运行结束时间 " + run.finishedAt + " 晚于 deadline " + deadline
                                + ", 补开并关闭一次超时事件(不重复计数)", incident);
            }
            // 之前已开过超时事件: 只关闭一次
            if (incident.closedAt == null) {
                closeIncident(incident, run.finishedAt, inTime);
            }
            if (inTime) {
                return new SlaEvaluation(SlaStatus.MET,
                        "尽管监控期曾观察到超时, 但运行结束时间 " + run.finishedAt
                                + " 不晚于 deadline " + deadline + ", 超时事件关闭为恢复", incident);
            }
            return new SlaEvaluation(SlaStatus.BREACHED,
                    "运行结束时间 " + run.finishedAt + " 晚于 deadline " + deadline
                            + ", 超时事件成立并关闭", incident);
        }

        // 进行中
        if (!observedAt.isAfter(deadline)) {
            return new SlaEvaluation(SlaStatus.PENDING,
                    "观察时刻 " + observedAt + " 未超过 deadline " + deadline + ", 剩余 "
                            + (deadline.toEpochMilli() - observedAt.toEpochMilli()) + "ms", incident);
        }
        if (incident == null) {
            incident = openIncident(run, deadline, observedAt);
            return new SlaEvaluation(SlaStatus.OPEN,
                    "观察时刻 " + observedAt + " 已超过 deadline " + deadline
                            + ", 超时事件开启(只开一次)", incident);
        }
        return new SlaEvaluation(SlaStatus.OPEN,
                "超时事件已于 " + incident.openedAt + " 开启, 重复观察不重复计数", incident);
    }

    /**
     * 只读分类: 与 {@link #evaluateLocked} 使用完全相同的判定规则, 但<b>不</b>开启/关闭任何
     * 超时事件, 供查询与聚合在任意观察时刻使用。结论同样只依赖业务时间, 故稳定可复现。
     */
    public SlaEvaluation classify(RunState run, Instant observedAt) {
        Instant deadline = deadline(run);
        if (deadline == null) {
            return new SlaEvaluation(SlaStatus.PENDING, "deadline 尚未确定", null);
        }
        TimeoutIncident incident = store.incidentFor(run.runId);
        if (run.status != RunStatus.RUNNING) {
            boolean inTime = !run.finishedAt.isAfter(deadline);
            if (inTime) {
                return new SlaEvaluation(SlaStatus.MET,
                        "结束时间 " + run.finishedAt + " 不晚于 deadline " + deadline, incident);
            }
            return new SlaEvaluation(SlaStatus.BREACHED,
                    "结束时间 " + run.finishedAt + " 晚于 deadline " + deadline, incident);
        }
        if (!observedAt.isAfter(deadline)) {
            return new SlaEvaluation(SlaStatus.PENDING,
                    "观察时刻 " + observedAt + " 未超过 deadline " + deadline, incident);
        }
        return new SlaEvaluation(SlaStatus.OPEN,
                "观察时刻 " + observedAt + " 已超过 deadline " + deadline, incident);
    }

    private TimeoutIncident openIncident(RunState run, Instant deadline, Instant openedAt) {        TimeoutIncident incident = new TimeoutIncident();
        incident.pipelineId = run.pipelineId;
        incident.runId = run.runId;
        incident.deadline = deadline;
        incident.openedAt = openedAt;
        incident.breached = false;
        store.putIncident(incident);
        return incident;
    }

    private void closeIncident(TimeoutIncident incident, Instant closedAt, boolean recovered) {
        incident.closedAt = closedAt;
        incident.breached = !recovered;
    }

    /**
     * 以当前时钟扫描所有进行中的运行, 推进 SLA(开启超时事件)。
     * 同时幂等关闭此前已结束但 incident 仍开启的运行(快照恢复后的补偿)。
     *
     * @return 本次扫描后处于 OPEN 的运行数
     */
    public int sweepNow() {
        Instant now = clock.instant();
        return store.withLock(() -> {
            int open = 0;
            boolean changed = false;
            for (RunState run : store.allRuns()) {
                SlaEvaluation evaluation = evaluateLocked(run, now);
                if (evaluation.status() == SlaStatus.OPEN) {
                    open++;
                }
                if (evaluation.incident() != null) {
                    changed = true;
                }
            }
            if (changed) {
                store.persistLocked();
            }
            return open;
        });
    }

    /** 对监控时刻的判定结果。 */
    public record SlaEvaluation(SlaStatus status, String reason, TimeoutIncident incident) {
    }
}
