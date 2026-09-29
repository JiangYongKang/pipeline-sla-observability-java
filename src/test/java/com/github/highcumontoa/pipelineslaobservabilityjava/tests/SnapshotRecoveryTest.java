package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.EventIngestionService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 从本地快照恢复后: 运行状态、SLA 结论、聚合指标必须与重启前一致;
 * 已接受过的 eventId 重启后仍然幂等/冲突可识别。
 */
class SnapshotRecoveryTest {

    private static final Instant T0 = Instant.parse("2026-09-29T00:00:00Z");
    private static final String P = "pipe-snap";

    @Test
    void stateAndMetricsAreIdenticalAfterSnapshotRestore() throws Exception {
        Path snapshot = Files.createTempFile("snap-restore-", ".json");
        Files.deleteIfExists(snapshot);
        Clock clock = Clock.fixed(T0.plusSeconds(120), ZoneOffset.UTC);

        // 第一个"进程": 写入一个 deadline 后结束的 run(产生已关闭的超时事件)
        RunStore store1 = new RunStore(new ObjectMapper(), snapshot.toString());
        SlaService sla1 = new SlaService(clock, store1);
        EventIngestionService ingestion1 = new EventIngestionService(store1, sla1);
        MetricsService metrics1 = new MetricsService(store1, sla1, clock);

        for (PipelineEvent e : List.of(
                Events.runStarted("s-e1", P, "srun", 1, T0, T0, 60_000),
                Events.attemptStarted("s-e2", P, "srun", 2, T0.plusSeconds(1), 1),
                Events.attemptSucceeded("s-e3", P, "srun", 3, T0.plusSeconds(90), 1, 42),
                Events.runSucceeded("s-e4", P, "srun", 4, T0.plusSeconds(90)))) {
            ingestion1.ingest(e);
        }
        RunView beforeView = metrics1.getRun("srun");
        MetricsView beforeMetrics = metrics1.pipelineMetrics(P);
        System.out.println("[重启前] status=" + beforeView.status() + " sla="
                + beforeView.slaStatus() + " processed=" + beforeView.processedCount());
        System.out.println("[重启前聚合] breached=" + beforeMetrics.timeoutIncidentsBreached()
                + " slaBreached=" + beforeMetrics.slaBreached());

        // 第二个"进程": 仅从快照加载, 不接收任何新事件
        RunStore store2 = new RunStore(new ObjectMapper(), snapshot.toString());
        SlaService sla2 = new SlaService(clock, store2);
        EventIngestionService ingestion2 = new EventIngestionService(store2, sla2);
        MetricsService metrics2 = new MetricsService(store2, sla2, clock);

        RunView afterView = metrics2.getRun("srun");
        MetricsView afterMetrics = metrics2.pipelineMetrics(P);
        System.out.println("[恢复后] status=" + afterView.status() + " sla="
                + afterView.slaStatus() + " processed=" + afterView.processedCount());
        System.out.println("[恢复后聚合] breached=" + afterMetrics.timeoutIncidentsBreached()
                + " slaBreached=" + afterMetrics.slaBreached());

        assertEquals(beforeView.status(), afterView.status());
        assertEquals(beforeView.slaStatus(), afterView.slaStatus());
        assertEquals(beforeView.attemptCount(), afterView.attemptCount());
        assertEquals(beforeView.processedCount(), afterView.processedCount());
        assertEquals(beforeView.durationMillis(), afterView.durationMillis());
        assertEquals(beforeMetrics.timeoutIncidentsBreached(),
                afterMetrics.timeoutIncidentsBreached());
        assertEquals(beforeMetrics.totalProcessedCount(), afterMetrics.totalProcessedCount());

        // 旧 eventId 仍幂等, 新内容复用旧 eventId 仍冲突
        PipelineEvent duplicate = Events.runStarted("s-e1", P, "srun", 1, T0, T0, 60_000);
        IngestResult dup = ingestion2.ingest(duplicate);
        System.out.println("[恢复后] 旧 eventId 重复提交 duplicate=" + dup.duplicate());
        assertEquals(true, dup.duplicate());

        Files.deleteIfExists(snapshot);
    }
}
