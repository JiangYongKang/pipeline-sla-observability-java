package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunRecord;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaPolicy;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaVerdict;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import com.github.highcumontoa.pipelineslaobservabilityjava.time.Clock;
import org.springframework.stereotype.Service;

/**
 * 新鲜度与 SLA 判定。
 *
 * <p>判定口径（截止时间 deadline = scheduledAt + timeoutMillis，只与调度周期有关）：
 * <ul>
 *   <li>{@link SlaStatus#ON_TRACK}：尚未到 deadline，运行未结束或正等待重试；</li>
 *   <li>{@link SlaStatus#ON_TIME}：在 deadline 之前（含边界）成功；</li>
 *   <li>{@link SlaStatus#TIMEOUT}：已过 deadline 仍未成功——包括终态失败、
 *       可重试失败后仍在跑/等待，以及 deadline 后仍未完成；</li>
 *   <li>{@link SlaStatus#LATE_SUCCESS}：deadline 之后才成功。
 *       违约事实一旦发生不可逆：迟到的 SUCCEEDED 只会把 TIMEOUT 收敛为 LATE_SUCCESS，
 *       绝不会翻回 ON_TIME，因此“短暂抖动/迟到完成”不会产生错误的恢复结论或重复计数。</li>
 * </ul>
 *
 * <p>结论是纯函数：同样的已接受事件集合 + 同一评估时刻，结论必然相同；
 * 评估动作本身不写入任何状态，因此反复评估幂等、不重复计数。
 */
@Service
public class SlaService {

    private final EventStore store;
    private final SlaPolicyRegistry policyRegistry;
    private final Clock clock;

    public SlaService(EventStore store, SlaPolicyRegistry policyRegistry, Clock clock) {
        this.store = store;
        this.policyRegistry = policyRegistry;
        this.clock = clock;
    }

    public SlaVerdict evaluate(String runId) {
        RunRecord rec;
        long now;
        synchronized (store) {
            rec = store.getRecord(runId);
            now = clock.nowMillis();
            if (rec == null) {
                return null;
            }
            return evaluate(rec, now, policyRegistry.policyFor(rec.getPipelineName()));
        }
    }

    /** 纯逻辑判定，包级可见以便测试直接构造场景。 */
    SlaVerdict evaluate(RunRecord rec, long now, SlaPolicy policy) {
        long scheduledAt = rec.getScheduledAt();
        long deadline = scheduledAt + policy.timeoutMillis();
        RunState state = rec.getState();

        if (state == RunState.SUCCEEDED) {
            long successAt = rec.getFinishedAt() == null ? now : rec.getFinishedAt();
            long delay = Math.max(0L, successAt - deadline);
            if (successAt <= deadline) {
                return verdict(rec, SlaStatus.ON_TIME, deadline, successAt, 0L, false, false,
                        "成功时间 " + successAt + " 未晚于截止时间 " + deadline
                                + "（延迟 " + (successAt - scheduledAt) + "ms，预算 "
                                + policy.timeoutMillis() + "ms）");
            }
            return verdict(rec, SlaStatus.LATE_SUCCESS, deadline, successAt, delay, true, true,
                    "成功时间 " + successAt + " 晚于截止时间 " + deadline + "，超时已事实发生；"
                            + "迟到成功计为违约恢复（LATE_SUCCESS），不回退为 ON_TIME，不重复计数");
        }

        if (now <= deadline) {
            long remaining = deadline - now;
            return verdict(rec, SlaStatus.ON_TRACK, deadline, null, null, false, false,
                    "当前时间 " + now + " 距截止时间 " + deadline + " 尚余 " + remaining
                            + "ms，状态=" + state + "，暂不判定超时");
        }

        long overrun = now - deadline;
        if (state == RunState.FAILED_FINAL) {
            return verdict(rec, SlaStatus.TIMEOUT, deadline, null, overrun, true, false,
                    "已过截止时间 " + overrun + "ms 且运行为终态失败，判定 TIMEOUT 且不可恢复");
        }
        // RUNNING / AWAITING_RETRY / SCHEDULED 且已过 deadline
        return verdict(rec, SlaStatus.TIMEOUT, deadline, null, overrun, true, false,
                "已过截止时间 " + overrun + "ms 仍未成功，状态=" + state
                        + "，判定 TIMEOUT；若之后成功将收敛为 LATE_SUCCESS");
    }

    private static SlaVerdict verdict(RunRecord rec, SlaStatus status, long deadline,
                                      Long successAt, Long delay,
                                      boolean timedOut, boolean recovered, String basis) {
        return new SlaVerdict(
                rec.getRunId(),
                rec.getPipelineName(),
                status,
                rec.getScheduledAt(),
                deadline,
                successAt,
                delay,
                timedOut,
                recovered,
                basis);
    }
}
