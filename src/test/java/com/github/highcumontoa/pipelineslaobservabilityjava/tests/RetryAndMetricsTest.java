package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.EventIngestionService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 失败/重试链路、终态失败、尝试次数/耗时/吞吐, 以及事件到达顺序无关性。
 */
class RetryAndMetricsTest {

    private static final Instant T0 = Instant.parse("2026-09-29T00:00:00Z");
    private static final String P = "pipe-retry";

    private record Harness(EventIngestionService ingestion, MetricsService metrics,
                           SlaService sla, RunStore store) {
    }

    private Harness harness() {
        Clock clock = Clock.fixed(T0.plusSeconds(3600), ZoneOffset.UTC);
        RunStore store = new RunStore(new ObjectMapper(), "");
        SlaService sla = new SlaService(clock, store);
        EventIngestionService ingestion = new EventIngestionService(store, sla);
        MetricsService metrics = new MetricsService(store, sla, clock);
        return new Harness(ingestion, metrics, sla, store);
    }

    /** 两次可重试失败, 第三次成功。 */
    private List<PipelineEvent> eventuallySucceeds(String runId) {
        return List.of(
                Events.runStarted(runId + "-e1", P, runId, 1, T0, T0, 600_000),
                Events.attemptStarted(runId + "-e2", P, runId, 2, T0.plusSeconds(1), 1),
                Events.attemptFailure(runId + "-e3", P, runId, 3, T0.plusSeconds(2), 1,
                        "E_TRANSIENT", 10),
                Events.attemptStarted(runId + "-e4", P, runId, 4, T0.plusSeconds(3), 2),
                Events.attemptFailure(runId + "-e5", P, runId, 5, T0.plusSeconds(4), 2,
                        "E_TRANSIENT", 20),
                Events.attemptStarted(runId + "-e6", P, runId, 6, T0.plusSeconds(5), 3),
                Events.attemptSucceeded(runId + "-e7", P, runId, 7, T0.plusSeconds(10), 3, 70),
                Events.runSucceeded(runId + "-e8", P, runId, 8, T0.plusSeconds(10))
        );
    }

    /** 两次可重试失败后终态失败。 */
    private List<PipelineEvent> eventuallyFails(String runId) {
        return List.of(
                Events.runStarted(runId + "-e1", P, runId, 1, T0, T0, 600_000),
                Events.attemptStarted(runId + "-e2", P, runId, 2, T0.plusSeconds(1), 1),
                Events.attemptFailure(runId + "-e3", P, runId, 3, T0.plusSeconds(2), 1,
                        "E_TRANSIENT", 5),
                Events.attemptStarted(runId + "-e4", P, runId, 4, T0.plusSeconds(3), 2),
                Events.attemptFailure(runId + "-e5", P, runId, 5, T0.plusSeconds(4), 2,
                        "E_GIVE_UP", 8),
                Events.runFailed(runId + "-e6", P, runId, 6, T0.plusSeconds(4), "E_GIVE_UP")
        );
    }

    @Test
    void retriableFailuresAreCountedUntilSuccess() {
        Harness h = harness();
        eventuallySucceeds("r-ok").forEach(e -> h.ingestion.ingest(e));
        RunView v = h.metrics.getRun("r-ok");

        System.out.println("[输入] 2 次 RETRIABLE_FAILURE 后第 3 次尝试成功");
        System.out.println("[判定] status=" + v.status() + " attempts=" + v.attemptCount()
                + " retriableFailures=" + v.retriableFailureCount()
                + " processed=" + v.processedCount()
                + " durationMs=" + v.durationMillis()
                + " throughput/s=" + v.throughputPerSec());

        assertEquals(RunStatus.SUCCEEDED, v.status());
        assertEquals(3, v.attemptCount());
        assertEquals(2, v.retriableFailureCount());
        // 10 + 20 + 70 = 100
        assertEquals(100L, v.processedCount());
        assertEquals(10_000L, v.durationMillis());
        assertEquals(10.0d, v.throughputPerSec(), 0.001);
        assertEquals(1000L, v.attempts().get(0).durationMillis());
        assertEquals(1000L, v.attempts().get(1).durationMillis());
        assertEquals(5000L, v.attempts().get(2).durationMillis());
    }

    @Test
    void terminalFailureIsDistinguishedFromRetriable() {
        Harness h = harness();
        eventuallyFails("r-bad").forEach(e -> h.ingestion.ingest(e));
        RunView v = h.metrics.getRun("r-bad");

        System.out.println("[输入] 2 次尝试均 RETRIABLE_FAILURE, 随后 RUN_FAILED");
        System.out.println("[判定] status=" + v.status() + " attempts=" + v.attemptCount()
                + " retriableFailures=" + v.retriableFailureCount());

        assertEquals(RunStatus.FAILED, v.status());
        assertEquals(2, v.attemptCount());
        assertEquals(2, v.retriableFailureCount());
    }

    @Test
    void runFailedWithoutRetriableFailureIsRejected() {
        Harness h = harness();
        h.ingestion.ingest(Events.runStarted("x-e1", P, "x", 1, T0, T0, 600_000));
        // 直接 RUN_FAILED 且前面没有 RETRIABLE_FAILURE
        PipelineEvent terminal = Events.runFailed("x-e2", P, "x", 2, T0.plusSeconds(1), "E");
        System.out.println("[输入] 无任何失败尝试直接 RUN_FAILED, 期望 ILLEGAL_TRANSITION");
        assertEquals(com.github.highcumontoa.pipelineslaobservabilityjava.domain.RejectReason.ILLEGAL_TRANSITION,
                org.junit.jupiter.api.Assertions.assertThrows(
                        com.github.highcumontoa.pipelineslaobservabilityjava.domain.EventRejectedException.class,
                        () -> h.ingestion.ingest(terminal)).getReason());
    }

    @Test
    void aggregationIsStableUnderDifferentEventArrivalOrder() {
        // 世界 A: 事件按 run 分组顺序到达
        Harness a = harness();
        List<PipelineEvent> ordered = new ArrayList<>();
        ordered.addAll(eventuallySucceeds("run-1"));
        ordered.addAll(eventuallyFails("run-2"));
        ordered.forEach(a.ingestion::ingest);
        MetricsView ma = a.metrics.pipelineMetrics(P);

        // 世界 B: 两个 run 的事件交错(但每个 run 内部序号仍递增), 通过批量接口提交
        Harness b = harness();
        List<PipelineEvent> r1 = eventuallySucceeds("run-1");
        List<PipelineEvent> r2 = eventuallyFails("run-2");
        List<PipelineEvent> interleaved = new ArrayList<>();
        int n = Math.max(r1.size(), r2.size());
        for (int i = 0; i < n; i++) {
            if (i < r1.size()) interleaved.add(r1.get(i));
            if (i < r2.size()) interleaved.add(r2.get(i));
        }
        List<IngestResult> results = b.ingestion.ingestBatch(interleaved);
        long batchAccepted = results.stream().filter(IngestResult::accepted).count();
        MetricsView mb = b.metrics.pipelineMetrics(P);

        System.out.println("[输入] 顺序到达 " + ordered.size() + " 事件; 交错到达 "
                + interleaved.size() + " 事件, 批量接受 " + batchAccepted);
        System.out.println("[判定A] runs=" + ma.totalRuns() + " ok=" + ma.succeededRuns()
                + " failed=" + ma.failedRuns() + " attempts=" + ma.totalAttempts()
                + " retriable=" + ma.retriableFailureCount());
        System.out.println("[判定B] runs=" + mb.totalRuns() + " ok=" + mb.succeededRuns()
                + " failed=" + mb.failedRuns() + " attempts=" + mb.totalAttempts()
                + " retriable=" + mb.retriableFailureCount());

        assertEquals(ordered.size(), batchAccepted);
        assertEquals(ma.totalRuns(), mb.totalRuns());
        assertEquals(ma.succeededRuns(), mb.succeededRuns());
        assertEquals(ma.failedRuns(), mb.failedRuns());
        assertEquals(ma.totalAttempts(), mb.totalAttempts());
        assertEquals(ma.retriableFailureCount(), mb.retriableFailureCount());
        assertEquals(ma.totalProcessedCount(), mb.totalProcessedCount());
        assertEquals(ma.totalRunDurationMillis(), mb.totalRunDurationMillis());
        assertEquals(ma.aggregateThroughputPerSec(), mb.aggregateThroughputPerSec(), 0.001);

        // 逐 run 比对, 证明结论只依赖最终状态
        for (String runId : List.of("run-1", "run-2")) {
            RunView va = a.metrics.getRun(runId);
            RunView vb = b.metrics.getRun(runId);
            assertNotNull(va);
            assertNotNull(vb);
            assertEquals(va.status(), vb.status());
            assertEquals(va.attemptCount(), vb.attemptCount());
            assertEquals(va.processedCount(), vb.processedCount());
            assertEquals(va.durationMillis(), vb.durationMillis());
        }
    }

    @Test
    void batchRejectionDoesNotStopOtherEvents() {
        Harness h = harness();
        PipelineEvent good = Events.runStarted("g-e1", P, "g", 1, T0, T0, 600_000);
        PipelineEvent bad = Events.runSucceeded("g-e2", P, "g", 2, T0.plusSeconds(1)); // 非法流转
        PipelineEvent good2 = Events.attemptStarted("g-e3", P, "g", 2, T0.plusSeconds(1), 1);
        List<IngestResult> results = h.ingestion.ingestBatch(List.of(good, bad, good2));
        System.out.println("[输入] 批量含 1 条非法流转, 期望逐条返回且其余继续");
        results.forEach(r -> System.out.println("[结果] event=" + r.eventId()
                + " accepted=" + r.accepted() + " reason=" + r.reason()));
        assertEquals(3, results.size());
        assertTrue(results.get(0).accepted());
        assertEquals(false, results.get(1).accepted());
        assertEquals("ILLEGAL_TRANSITION", results.get(1).reason());
        assertTrue(results.get(2).accepted());
    }
}
