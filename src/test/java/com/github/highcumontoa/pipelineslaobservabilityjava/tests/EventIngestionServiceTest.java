package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.EventRejectedException;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RejectReason;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.EventIngestionService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * 事件接入: 正常流转、重复提交、状态回退与乱序缺口。
 * 每个断言前打印输入事件与期望判定依据, 便于解释。
 */
class EventIngestionServiceTest {

    private static final Instant T0 = Instant.parse("2026-09-29T00:00:00Z");
    private static final String P = "pipe-a";

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

    @Test
    void happyPathSucceededRun() {
        Harness h = harness();
        PipelineEvent start = Events.runStarted("e1", P, "r1", 1, T0, T0, 60_000);
        PipelineEvent a1 = Events.attemptStarted("e2", P, "r1", 2, T0.plusMillis(100), 1);
        PipelineEvent ok = Events.attemptSucceeded("e3", P, "r1", 3, T0.plusMillis(5_000), 1, 100);
        PipelineEvent done = Events.runSucceeded("e4", P, "r1", 4, T0.plusMillis(5_000));
        for (PipelineEvent e : new PipelineEvent[]{start, a1, ok, done}) {
            System.out.println("[输入事件] " + e.type() + " eventId=" + e.eventId()
                    + " seq=" + e.seq() + " at=" + e.occurredAt());
            assertTrue(h.ingestion.ingest(e).accepted(), "应被接受: " + e.type());
        }
        RunView view = h.metrics.getRun("r1");
        System.out.println("[判定] status=" + view.status() + " sla=" + view.slaStatus()
                + " attempts=" + view.attemptCount() + " 依据=" + view.slaReason());
        assertEquals(RunStatus.SUCCEEDED, view.status());
        assertEquals(1, view.attemptCount());
        assertEquals(0, view.retriableFailureCount());
    }

    @Test
    void duplicateEventIsIdempotentButConflictIsRejected() {
        Harness h = harness();
        PipelineEvent start = Events.runStarted("e1", P, "r2", 1, T0, T0, 60_000);
        h.ingestion.ingest(start);

        // 完全相同的重复提交: 幂等, 不覆盖任何状态
        PipelineEvent duplicate = Events.runStarted("e1", P, "r2", 1, T0, T0, 60_000);
        System.out.println("[输入事件] 重复提交相同 eventId=e1, 期望幂等 duplicate=true");
        IngestResult result = h.ingestion.ingest(duplicate);
        assertTrue(result.accepted());
        assertTrue(result.duplicate(), "相同事件重复提交必须是幂等重复");

        // 同一 eventId 但内容不同: 按 EVENT_ID_CONFLICT 拒绝
        PipelineEvent conflict = Events.attemptStarted("e1", P, "r2", 2, T0.plusMillis(10), 1);
        System.out.println("[输入事件] eventId=e1 但类型变为 ATTEMPT_STARTED, 期望 EVENT_ID_CONFLICT");
        EventRejectedException ex = assertThrows(EventRejectedException.class,
                () -> h.ingestion.ingest(conflict));
        assertEquals(RejectReason.EVENT_ID_CONFLICT, ex.getReason());
    }

    @Test
    void duplicateRunStartIsRejected() {
        Harness h = harness();
        h.ingestion.ingest(Events.runStarted("e1", P, "r3", 1, T0, T0, 60_000));
        PipelineEvent second = Events.runStarted("e9", P, "r3", 2, T0.plusMillis(5), T0, 60_000);
        System.out.println("[输入事件] 同一 runId=r3 的第二个 RUN_STARTED, 期望 DUPLICATE_RUN");
        EventRejectedException ex = assertThrows(EventRejectedException.class,
                () -> h.ingestion.ingest(second));
        assertEquals(RejectReason.DUPLICATE_RUN, ex.getReason());
    }

    @Test
    void eventForUnknownRunIsRejected() {
        Harness h = harness();
        PipelineEvent orphan = Events.attemptStarted("e1", P, "ghost", 1, T0, 1);
        System.out.println("[输入事件] 无 RUN_STARTED 的 runId=ghost, 期望 UNKNOWN_RUN");
        EventRejectedException ex = assertThrows(EventRejectedException.class,
                () -> h.ingestion.ingest(orphan));
        assertEquals(RejectReason.UNKNOWN_RUN, ex.getReason());
    }

    @Test
    void illegalTransitionsAreRejectedByReason() {
        Harness h = harness();
        h.ingestion.ingest(Events.runStarted("e1", P, "r4", 1, T0, T0, 60_000));
        h.ingestion.ingest(Events.attemptStarted("e2", P, "r4", 2, T0.plusMillis(10), 1));

        // 没有成功的 attempt 就 RUN_SUCCEEDED: ILLEGAL_TRANSITION
        PipelineEvent premature = Events.runSucceeded("e3", P, "r4", 3, T0.plusMillis(20));
        System.out.println("[输入事件] 无成功尝试直接 RUN_SUCCEEDED, 期望 ILLEGAL_TRANSITION");
        assertEquals(RejectReason.ILLEGAL_TRANSITION,
                assertThrows(EventRejectedException.class, () -> h.ingestion.ingest(premature))
                        .getReason());

        // 引用不存在的 attempt
        PipelineEvent badAttempt = Events.attemptSucceeded("e4", P, "r4", 3,
                T0.plusMillis(30), 9, 1);
        System.out.println("[输入事件] 结束 attemptSeq=9(不存在), 期望 UNKNOWN_ATTEMPT");
        assertEquals(RejectReason.UNKNOWN_ATTEMPT,
                assertThrows(EventRejectedException.class, () -> h.ingestion.ingest(badAttempt))
                        .getReason());

        // 正常结束尝试与运行
        h.ingestion.ingest(Events.attemptSucceeded("e5", P, "r4", 3,
                T0.plusMillis(40), 1, 10));
        h.ingestion.ingest(Events.runSucceeded("e6", P, "r4", 4, T0.plusMillis(40)));

        // 终态之后再来事件: ILLEGAL_TRANSITION
        PipelineEvent afterDeath = Events.attemptStarted("e7", P, "r4", 5,
                T0.plusMillis(50), 2);
        System.out.println("[输入事件] 终态后再 ATTEMPT_STARTED, 期望 ILLEGAL_TRANSITION");
        assertEquals(RejectReason.ILLEGAL_TRANSITION,
                assertThrows(EventRejectedException.class, () -> h.ingestion.ingest(afterDeath))
                        .getReason());
    }

    @Test
    void finishedAttemptCannotBeReportedTwice() {
        Harness h = harness();
        h.ingestion.ingest(Events.runStarted("e1", P, "r5", 1, T0, T0, 60_000));
        h.ingestion.ingest(Events.attemptStarted("e2", P, "r5", 2, T0.plusMillis(10), 1));
        h.ingestion.ingest(Events.attemptSucceeded("e3", P, "r5", 3,
                T0.plusMillis(20), 1, 5));
        PipelineEvent again = Events.attemptSucceeded("e4", P, "r5", 4,
                T0.plusMillis(30), 1, 5);
        System.out.println("[输入事件] attempt=1 再次上报成功, 期望 ATTEMPT_ALREADY_FINISHED");
        assertEquals(RejectReason.ATTEMPT_ALREADY_FINISHED,
                assertThrows(EventRejectedException.class, () -> h.ingestion.ingest(again))
                        .getReason());
    }

    @Test
    void outOfOrderGapIsHeldThenDrainedInSequence() {
        Harness h = harness();
        h.ingestion.ingest(Events.runStarted("e1", P, "r6", 1, T0, T0, 60_000));

        PipelineEvent seq3 = Events.attemptStarted("e3", P, "r6", 3, T0.plusMillis(20), 1);
        System.out.println("[输入事件] seq=3 先到(缺 seq=2), 期望 OUT_OF_ORDER_GAP 暂存");
        EventRejectedException ex = assertThrows(EventRejectedException.class,
                () -> h.ingestion.ingest(seq3));
        assertEquals(RejectReason.OUT_OF_ORDER_GAP, ex.getReason());
        assertEquals(1, h.ingestion.pendingCount("r6"), "乱序事件必须进入暂存而非被覆盖");

        PipelineEvent seq2 = Events.attemptStarted("e2", P, "r6", 2, T0.plusMillis(10), 1);
        System.out.println("[输入事件] seq=2 补齐, 期望 seq=2 接受且暂存的 seq=3 自动重放");
        // seq2 是 ATTEMPT_STARTED attemptSeq=1; 暂存的 seq3 也是 ATTEMPT_STARTED attemptSeq=1,
        // 重放时因 attempt 编号不接续被 ILLEGAL_TRANSITION 拒绝, 不应污染状态
        h.ingestion.ingest(seq2);
        RunView view = h.metrics.getRun("r6");
        System.out.println("[判定] 补齐后 attempts=" + view.attemptCount()
                + " (非法重放不得写入第二个相同 attempt)");
        assertEquals(1, view.attemptCount());

        // 旧序号重放必须被 STALE_SEQUENCE 拒绝
        PipelineEvent stale = Events.attemptStarted("e10", P, "r6", 1, T0.plusMillis(5), 1);
        System.out.println("[输入事件] seq=1 的旧事件, 期望 STALE_SEQUENCE");
        assertEquals(RejectReason.STALE_SEQUENCE,
                assertThrows(EventRejectedException.class, () -> h.ingestion.ingest(stale))
                        .getReason());
    }

    @Test
    void timestampRegressionIsRejected() {
        Harness h = harness();
        h.ingestion.ingest(Events.runStarted("e1", P, "r7", 1, T0.plusSeconds(10), T0, 60_000));
        PipelineEvent back = Events.attemptStarted("e2", P, "r7", 2, T0, 1);
        System.out.println("[输入事件] occurredAt 早于上一事件, 期望 TIMESTAMP_REGRESSION");
        assertEquals(RejectReason.TIMESTAMP_REGRESSION,
                assertThrows(EventRejectedException.class, () -> h.ingestion.ingest(back))
                        .getReason());
        RunView view = h.metrics.getRun("r7");
        System.out.println("[判定] 时间倒退事件被拒后 attempts=" + view.attemptCount()
                + " (必须为 0, 状态未被污染)");
        assertEquals(0, view.attemptCount());
    }
}
