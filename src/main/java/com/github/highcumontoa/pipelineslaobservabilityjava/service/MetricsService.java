package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.SlaStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.AttemptState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.TimeoutIncident;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService.SlaEvaluation;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 指标聚合与查询服务。
 *
 * <p>聚合只读取已接受事件形成的最终状态(及已落盘的超时事件), 不依赖事件到达先后;
 * {@link RunStore} 快照保存的也是同一批状态, 因此重启恢复后聚合结果不变。
 */
@Service
public class MetricsService {

    private final RunStore store;
    private final SlaService slaService;
    private final Clock clock;

    public MetricsService(RunStore store, SlaService slaService, Clock clock) {
        this.store = store;
        this.slaService = slaService;
        this.clock = clock;
    }

    /** 在指定观察时刻下计算运行视图(纯读取, 不修改状态)。 */
    public RunView toRunView(RunState run, Instant observedAt) {
        Instant deadline = slaService.deadline(run);
        long freshness = slaService.freshnessDelayMillis(run);
        SlaEvaluation evaluation = slaService.classify(run, observedAt);

        List<AttemptView> attempts = run.attempts.values().stream()
                .sorted(Comparator.comparingLong(a -> a.attemptSeq))
                .map(this::toAttemptView)
                .toList();

        int attemptCount = run.attempts.size();
        int retriable = (int) run.attempts.values().stream()
                .filter(a -> a.outcome == com.github.highcumontoa.pipelineslaobservabilityjava.domain.AttemptOutcome.RETRIABLE_FAILURE)
                .count();

        // 运行耗时: 终态用 finishedAt-startedAt; 进行中用观察时刻-startedAt
        long duration;
        boolean finished = run.status != RunStatus.RUNNING;
        if (finished) {
            duration = run.finishedAt.toEpochMilli() - run.startedAt.toEpochMilli();
        } else {
            duration = Math.max(0, observedAt.toEpochMilli() - run.startedAt.toEpochMilli());
        }
        long processed = run.attempts.values().stream().mapToLong(a -> a.processedCount).sum();
        Double throughput = duration > 0 ? (processed * 1000.0d / duration) : null;

        return new RunView(
                run.pipelineId,
                run.runId,
                run.status,
                run.scheduledTime,
                run.startedAt,
                run.finishedAt,
                run.slaMillis,
                deadline,
                freshness,
                evaluation.status(),
                evaluation.reason(),
                attemptCount,
                retriable,
                finished ? duration : null,
                processed,
                throughput,
                attempts
        );
    }

    private AttemptView toAttemptView(AttemptState a) {
        Long duration = (a.startedAt != null && a.finishedAt != null)
                ? a.finishedAt.toEpochMilli() - a.startedAt.toEpochMilli()
                : null;
        return new AttemptView(a.attemptSeq, a.startedAt, a.finishedAt, duration,
                a.outcome, a.errorCode, a.processedCount);
    }

    public RunView getRun(String runId) {
        Instant now = clock.instant();
        return store.withLock(() -> {
            RunState run = store.findRun(runId);
            return run == null ? null : toRunView(run, now);
        });
    }

    public List<RunView> allRuns() {
        Instant now = clock.instant();
        return store.withLock(() -> store.allRuns().stream()
                .sorted(Comparator.comparing(r -> r.runId))
                .map(run -> toRunView(run, now))
                .toList());
    }

    public MetricsView pipelineMetrics(String pipelineId) {
        Instant now = clock.instant();
        return store.withLock(() -> {
            List<RunState> runs = store.allRuns().stream()
                    .filter(r -> r.pipelineId.equals(pipelineId))
                    .sorted(Comparator.comparing(r -> r.runId))
                    .toList();

            List<RunView> views = runs.stream().map(r -> toRunView(r, now)).toList();

            int running = 0, succeeded = 0, failed = 0;
            int pending = 0, open = 0, met = 0, breached = 0;
            int totalAttempts = 0, retriableFailures = 0;
            long processed = 0;
            long totalDuration = 0;
            for (RunView v : views) {
                switch (v.status()) {
                    case RUNNING -> running++;
                    case SUCCEEDED -> succeeded++;
                    case FAILED -> failed++;
                }
                switch (v.slaStatus()) {
                    case PENDING -> pending++;
                    case OPEN -> open++;
                    case MET -> met++;
                    case BREACHED -> breached++;
                }
                totalAttempts += v.attemptCount();
                retriableFailures += v.retriableFailureCount();
                processed += v.processedCount() == null ? 0 : v.processedCount();
                if (v.durationMillis() != null) {
                    totalDuration += v.durationMillis();
                }
            }

            // 超时事件计数以落盘的 incident 为唯一口径, 每个 run 至多一条
            int incidentsOpened = 0, incidentsBreached = 0, incidentsRecovered = 0;
            for (TimeoutIncident incident : store.allIncidents()) {
                if (!pipelineId.equals(incident.pipelineId)) {
                    continue;
                }
                incidentsOpened++;
                if (incident.closedAt == null) {
                    // 仍开启中
                } else if (incident.breached) {
                    incidentsBreached++;
                } else {
                    incidentsRecovered++;
                }
            }

            Double aggregateThroughput = totalDuration > 0
                    ? processed * 1000.0d / totalDuration : null;

            return new MetricsView(
                    pipelineId,
                    runs.size(),
                    running,
                    succeeded,
                    failed,
                    retriableFailures,
                    totalAttempts,
                    pending,
                    open,
                    met,
                    breached,
                    incidentsOpened,
                    incidentsBreached,
                    incidentsRecovered,
                    processed,
                    totalDuration == 0 ? null : totalDuration,
                    aggregateThroughput,
                    views
            );
        });
    }
}
