package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.AggregationResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.Attempt;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunRecord;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaPolicy;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaVerdict;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import com.github.highcumontoa.pipelineslaobservabilityjava.time.Clock;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 失败/重试链路与吞吐指标聚合。
 *
 * <p>稳定性保证：聚合输入为“已接受事件集合归约出的运行记录”，
 * 所有指标都是可交换的求和/计数（总尝试数、总耗时、总条目数），
 * 因此事件到达顺序不同、或先从本地快照重放再聚合，结果完全一致。
 *
 * <p>口径：
 * <ul>
 *   <li>窗口归属按 scheduledAt（调度周期）过滤；</li>
 *   <li>总尝试数 = 各运行 STARTED 接受次数（重复事件不去重计数，但也不会被接受两次）；</li>
 *   <li>运行耗时 = finishedAt - firstStartedAt（未结束的运行不计入耗时均值）；</li>
 *   <li>吞吐 = 各已结束尝试 itemCount / attemptDuration 之和（items/sec），
 *       0 耗时按 1ms 兜底防止除零；</li>
 *   <li>SLA 计数复用 {@link SlaService} 的纯判定，保证聚合与单查口径一致。</li>
 * </ul>
 */
@Service
public class MetricsService {

    private final EventStore store;
    private final SlaPolicyRegistry policyRegistry;
    private final SlaService slaService;
    private final Clock clock;

    public MetricsService(EventStore store,
                          SlaPolicyRegistry policyRegistry,
                          SlaService slaService,
                          Clock clock) {
        this.store = store;
        this.policyRegistry = policyRegistry;
        this.slaService = slaService;
        this.clock = clock;
    }

    public AggregationResult aggregate(String pipelineName, long fromMillis, long toMillis) {
        if (toMillis < fromMillis) {
            throw new IllegalArgumentException("toMillis 不能早于 fromMillis");
        }
        List<RunRecord> snapshot;
        long now;
        synchronized (store) {
            snapshot = store.allRecords();
            now = clock.nowMillis();
        }

        int total = 0;
        int succeeded = 0;
        int finalFailed = 0;
        int timeouts = 0;
        int lateSuccess = 0;
        int retryableFailures = 0;
        long totalAttempts = 0;
        long finishedRuns = 0;
        long totalRunDuration = 0L;
        double throughputSum = 0.0;
        long throughputAttempts = 0;

        for (RunRecord rec : snapshot) {
            if (!pipelineName.equals(rec.getPipelineName())) {
                continue;
            }
            if (rec.getScheduledAt() < fromMillis || rec.getScheduledAt() > toMillis) {
                continue;
            }
            total++;
            if (rec.getState() == RunState.SUCCEEDED) {
                succeeded++;
            }
            if (rec.getState() == RunState.FAILED_FINAL) {
                finalFailed++;
            }

            SlaPolicy policy = policyRegistry.policyFor(pipelineName);
            SlaVerdict verdict = slaService.evaluate(rec, now, policy);
            if (verdict.status() == SlaStatus.TIMEOUT) {
                timeouts++;
            }
            if (verdict.status() == SlaStatus.LATE_SUCCESS) {
                lateSuccess++;
            }

            for (Attempt a : rec.getAttempts()) {
                totalAttempts++;
                if (a.isRetryableFailure()) {
                    retryableFailures++;
                }
                if (a.getFinishedAt() != null && a.getStartedAt() != null) {
                    long dur = Math.max(1L, a.getFinishedAt() - a.getStartedAt());
                    throughputSum += (double) a.getItemCount() * 1000.0 / dur;
                    throughputAttempts++;
                }
            }

            if (rec.getFinishedAt() != null && rec.getFirstStartedAt() != null
                    && rec.getFinishedAt() >= rec.getFirstStartedAt()) {
                finishedRuns++;
                totalRunDuration += rec.getFinishedAt() - rec.getFirstStartedAt();
            }
        }

        double avgAttempts = total == 0 ? 0.0 : (double) totalAttempts / total;
        Long avgRunDuration = finishedRuns == 0 ? null : totalRunDuration / finishedRuns;
        Double avgThroughput = throughputAttempts == 0 ? null
                : throughputSum / throughputAttempts;
        String basis = "窗口[" + fromMillis + "," + toMillis + "]按 scheduledAt 归属，共 " + total
                + " 个运行；总尝试=" + totalAttempts + "（已去重）；运行耗时样本=" + finishedRuns
                + "；吞吐样本=" + throughputAttempts
                + "；SLA 以 deadline=scheduledAt+timeout 纯事件判定，与到达顺序无关";

        return new AggregationResult(
                pipelineName,
                fromMillis,
                toMillis,
                total,
                succeeded,
                finalFailed,
                timeouts,
                lateSuccess,
                retryableFailures,
                totalAttempts,
                round2(avgAttempts),
                finishedRuns == 0 ? null : totalRunDuration,
                avgRunDuration,
                round2(throughputSum),
                avgThroughput == null ? null : round2(avgThroughput),
                basis);
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
