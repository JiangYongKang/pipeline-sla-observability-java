package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.SlaStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.TimeoutIncident;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.EventIngestionService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * SLA 与新鲜度边界情形。所有断言打印输入事件的业务时间与判定依据。
 */
class SlaBoundaryTest {

    private static final Instant T0 = Instant.parse("2026-09-29T00:00:00Z");
    private static final String P = "pipe-sla";

    private record Harness(EventIngestionService ingestion, MetricsService metrics,
                           SlaService sla, RunStore store, Clock clock) {
    }

    private Harness harness(Instant fixedNow) {
        Clock clock = Clock.fixed(fixedNow, ZoneOffset.UTC);
        RunStore store = new RunStore(new ObjectMapper(), "");
        SlaService sla = new SlaService(clock, store);
        EventIngestionService ingestion = new EventIngestionService(store, sla);
        MetricsService metrics = new MetricsService(store, sla, clock);
        return new Harness(ingestion, metrics, sla, store, clock);
    }

    /** 构造: 启动 + 一次成功尝试 + 运行成功。startedAt 固定为 scheduled(零新鲜度抖动)。 */
    private void succeedAt(Harness h, String runId, Instant scheduled, long slaMillis,
                           Instant finishedAt) {
        h.ingestion.ingest(Events.runStarted(runId + "-e1", P, runId, 1,
                scheduled, scheduled, slaMillis));
        h.ingestion.ingest(Events.attemptStarted(runId + "-e2", P, runId, 2,
                scheduled.plusMillis(10), 1));
        h.ingestion.ingest(Events.attemptSucceeded(runId + "-e3", P, runId, 3,
                finishedAt, 1, 10));
        h.ingestion.ingest(Events.runSucceeded(runId + "-e4", P, runId, 4, finishedAt));
    }

    @Test
    void finishExactlyOnDeadlineIsMetOneMillisLateIsBreached() {
        // run A: scheduled=T0, sla=60s, finishedAt=T0+60s 恰好等于 deadline
        Harness ha = harness(T0.plusSeconds(3600));
        succeedAt(ha, "a", T0, 60_000, T0.plusSeconds(60));
        RunView va = ha.metrics.getRun("a");
        System.out.println("[输入 A] deadline=" + va.deadline() + " finishedAt="
                + va.finishedAt());
        System.out.println("[判定 A] sla=" + va.slaStatus() + " 依据=" + va.slaReason());
        assertEquals(SlaStatus.MET, va.slaStatus(), "结束时刻等于 deadline 应视为未超时(<=)");

        // run B: 晚 1ms
        Harness hb = harness(T0.plusSeconds(3600));
        succeedAt(hb, "b", T0, 60_000, T0.plusMillis(60_001));
        RunView vb = hb.metrics.getRun("b");
        System.out.println("[输入 B] deadline=" + vb.deadline() + " finishedAt="
                + vb.finishedAt());
        System.out.println("[判定 B] sla=" + vb.slaStatus() + " 依据=" + vb.slaReason());
        assertEquals(SlaStatus.BREACHED, vb.slaStatus(), "晚 deadline 1ms 必须判超时");
    }

    @Test
    void freshnessDelayIsScheduledToActualStart() {
        Harness h = harness(T0.plusSeconds(3600));
        // scheduled=T0, 实际启动 T0+100s
        Instant start = T0.plusSeconds(100);
        h.ingestion.ingest(Events.runStarted("fresh-e1", P, "fresh", 1,
                start, T0, 60_000));
        h.ingestion.ingest(Events.attemptStarted("fresh-e2", P, "fresh", 2,
                start.plusMillis(10), 1));
        h.ingestion.ingest(Events.attemptSucceeded("fresh-e3", P, "fresh", 3,
                start.plusSeconds(10), 1, 10));
        h.ingestion.ingest(Events.runSucceeded("fresh-e4", P, "fresh", 4,
                start.plusSeconds(10)));
        RunView v = h.metrics.getRun("fresh");
        System.out.println("[输入] scheduled=" + v.scheduledTime() + " started=" + v.startedAt());
        System.out.println("[判定] freshnessDelay=" + v.freshnessDelayMillis() + "ms");
        assertEquals(100_000L, v.freshnessDelayMillis());
    }

    @Test
    void timeoutOpensOnceAndLateCompletionEventClosesAsRecoveryWithoutDoubleCount() {
        // 业务实际在 deadline 前完成, 但完成事件迟到: 先在 T0+120s 扫描到 OPEN, 再补完成事件
        Harness h = harness(T0.plusSeconds(120));
        h.ingestion.ingest(Events.runStarted("r-e1", P, "late", 1, T0, T0, 60_000));
        h.ingestion.ingest(Events.attemptStarted("r-e2", P, "late", 2,
                T0.plusSeconds(1), 1));

        // 第一次扫描: 已过 deadline -> OPEN, 开启一个 incident
        int open1 = h.sla.sweepNow();
        TimeoutIncident incident1 = h.store.incidentFor("late");
        System.out.println("[扫描1] now=" + T0.plusSeconds(120) + " openRuns=" + open1
                + " incident.openedAt=" + (incident1 == null ? null : incident1.openedAt));
        assertEquals(1, open1);
        assertNotNull(incident1);
        assertNull(incident1.closedAt);
        assertEquals(SlaStatus.OPEN, h.metrics.getRun("late").slaStatus());

        // 再次扫描不得重复开启
        int open2 = h.sla.sweepNow();
        TimeoutIncident incident2 = h.store.incidentFor("late");
        System.out.println("[扫描2] openRuns=" + open2 + " 同一 incident="
                + (incident1 == incident2));
        assertEquals(1, open2);
        assertEquals(incident1, incident2, "超时事件只能开启一次");

        // 迟到的完成事件: 业务时间 finishedAt=T0+30s(早于 deadline), 晚于上一事件 T0+1s, 合法
        h.ingestion.ingest(Events.attemptSucceeded("r-e3", P, "late", 3,
                T0.plusSeconds(30), 1, 50));
        h.ingestion.ingest(Events.runSucceeded("r-e4", P, "late", 4, T0.plusSeconds(30)));

        RunView view = h.metrics.getRun("late");
        TimeoutIncident closed = h.store.incidentFor("late");
        System.out.println("[迟到完成] finishedAt=" + view.finishedAt()
                + " sla=" + view.slaStatus() + " 依据=" + view.slaReason());
        System.out.println("[incident] closedAt=" + closed.closedAt + " breached="
                + closed.breached + " (关闭为恢复, 不计为超时成立)");
        assertEquals(SlaStatus.MET, view.slaStatus(),
                "判定以业务时间为准: deadline 前完成 => MET");
        assertEquals(T0.plusSeconds(30), closed.closedAt);
        assertEquals(false, closed.breached, "deadline 前完成 => 超时事件恢复, 非超时成立");

        MetricsView mv = h.metrics.pipelineMetrics(P);
        System.out.println("[聚合] opened=" + mv.timeoutIncidentsOpened()
                + " breached=" + mv.timeoutIncidentsBreached()
                + " recovered=" + mv.timeoutIncidentsRecovered()
                + " slaMet=" + mv.slaMet() + " slaOpen=" + mv.slaOpen());
        assertEquals(1, mv.timeoutIncidentsOpened());
        assertEquals(0, mv.timeoutIncidentsBreached());
        assertEquals(1, mv.timeoutIncidentsRecovered());
        assertEquals(1, mv.slaMet());
        assertEquals(0, mv.slaOpen());
    }

    @Test
    void lateFinishedBeyondDeadlineClosesAsBreached() {
        // 先扫描开启 incident, 再收到业务时间已过 deadline 的完成事件 => BREACHED
        Harness h = harness(T0.plusSeconds(120));
        h.ingestion.ingest(Events.runStarted("r-e1", P, "slow", 1, T0, T0, 60_000));
        h.ingestion.ingest(Events.attemptStarted("r-e2", P, "slow", 2,
                T0.plusSeconds(1), 1));
        h.sla.sweepNow();

        h.ingestion.ingest(Events.attemptSucceeded("r-e3", P, "slow", 3,
                T0.plusSeconds(90), 1, 50));
        h.ingestion.ingest(Events.runSucceeded("r-e4", P, "slow", 4, T0.plusSeconds(90)));

        RunView view = h.metrics.getRun("slow");
        TimeoutIncident incident = h.store.incidentFor("slow");
        System.out.println("[输入] finishedAt=" + view.finishedAt() + " deadline="
                + view.deadline());
        System.out.println("[判定] sla=" + view.slaStatus() + " incident.breached="
                + incident.breached);
        assertEquals(SlaStatus.BREACHED, view.slaStatus());
        assertEquals(true, incident.breached);

        MetricsView mv = h.metrics.pipelineMetrics(P);
        assertEquals(1, mv.timeoutIncidentsBreached());
        assertEquals(0, mv.timeoutIncidentsRecovered());
    }
}
