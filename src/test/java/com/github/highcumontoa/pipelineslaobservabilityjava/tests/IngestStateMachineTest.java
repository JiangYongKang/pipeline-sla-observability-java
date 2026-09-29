package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RejectReason;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunQueryService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaPolicyRegistry;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import com.github.highcumontoa.pipelineslaobservabilityjava.support.Events;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 乱序到达、状态回退、重复提交的拒绝分类测试。
 * 每个断言打印输入事件与判定依据。
 */
class IngestStateMachineTest {

    private IngestService ingest;
    private RunQueryService query;

    @BeforeEach
    void setUp(@TempDir Path tmp) {
        EventStore store = new EventStore(new ObjectMapper(), tmp.resolve("e.jsonl").toString());
        SlaPolicyRegistry registry = new SlaPolicyRegistry(60_000, 3);
        ingest = new IngestService(store, registry);
        query = new RunQueryService(store);
    }

    private void assertRejected(PipelineEvent e, RejectReason expected) {
        IngestResult r = ingest.ingest(e);
        System.out.println("[输入] " + brief(e) + " => accepted=" + r.accepted()
                + " duplicate=" + r.duplicate() + " reason=" + r.reason()
                + " state=" + r.runState() + " 依据: " + r.message());
        assertFalse(r.accepted(), "事件必须被拒绝: " + brief(e));
        assertEquals(expected, r.reason(),
                "拒绝原因必须可区分。输入=" + brief(e) + " 依据=" + r.message());
    }

    @Test
    void firstEventMustBeScheduled() {
        assertRejected(Events.started("x1", "r1", "p", 1, 10L),
                RejectReason.UNKNOWN_RUN);
        assertRejected(Events.succeeded("x2", "r1", "p", 1, 10L),
                RejectReason.UNKNOWN_RUN);
    }

    @Test
    void duplicateEventIsIdempotentAndNotSilentlyAccepted() {
        ingest.ingest(Events.scheduled("s1", "r2", "p", 0L));
        PipelineEvent dup = Events.scheduled("s1", "r2", "p", 0L);
        IngestResult r = ingest.ingest(dup);
        System.out.println("[输入] 重复 SCHEDULED => " + r.reason() + " duplicate=" + r.duplicate()
                + " 依据: " + r.message());
        assertFalse(r.accepted());
        assertTrue(r.duplicate());
        assertEquals(RejectReason.DUPLICATE_EVENT, r.reason());
        assertEquals(RunState.SCHEDULED, query.get("r2").state());

        // 同一 eventId 绑定到别的 runId —— 串线，也不能静默接受
        IngestResult cross = ingest.ingest(Events.scheduled("s1", "r2-other", "p", 1L));
        System.out.println("[输入] 同 eventId 换 runId => " + cross.reason()
                + " 依据: " + cross.message());
        assertFalse(cross.accepted());
        assertEquals(RejectReason.DUPLICATE_EVENT, cross.reason());
    }

    @Test
    void secondScheduledIsStateRegression() {
        ingest.ingest(Events.scheduled("s1", "r3", "p", 0L));
        assertRejected(Events.scheduled("s2", "r3", "p", 5L),
                RejectReason.ILLEGAL_TRANSITION);
    }

    @Test
    void completionWithoutMatchingStartedIsAttemptMismatch() {
        ingest.ingest(Events.scheduled("s1", "r4", "p", 0L));
        // 乱序：完成事件先于开始事件到达
        assertRejected(Events.succeeded("ok", "r4", "p", 1, 50L),
                RejectReason.ATTEMPT_MISMATCH);
        assertRejected(Events.failed("f1", "r4", "p", 2, 50L, true, "x"),
                RejectReason.ATTEMPT_MISMATCH);
        // 合法补齐后，迟到的完成事件（attempt=2，但当前是 attempt=1）仍必须拒绝
        assertTrue(ingest.ingest(Events.started("st1", "r4", "p", 1, 10L)).accepted());
        assertRejected(Events.succeeded("ok-late", "r4", "p", 2, 60L),
                RejectReason.ATTEMPT_MISMATCH);
        // 正确的 attempt=1 完成事件正常接受
        assertTrue(ingest.ingest(Events.succeeded("ok1", "r4", "p", 1, 70L)).accepted());
        assertEquals(RunState.SUCCEEDED, query.get("r4").state());
    }

    @Test
    void eventsAfterTerminalStateAreRejected() {
        ingest.ingest(Events.scheduled("s1", "r5", "p", 0L));
        ingest.ingest(Events.started("t1", "r5", "p", 1, 10L));
        ingest.ingest(Events.succeeded("o1", "r5", "p", 1, 20L));
        // 成功终态后的任何事件：回退 STARTED、再 SCHEDULED、重复完成都拒绝
        assertRejected(Events.started("t2", "r5", "p", 2, 30L),
                RejectReason.TERMINAL_RUN);
        assertRejected(Events.scheduled("s2", "r5", "p", 30L),
                RejectReason.TERMINAL_RUN);
        assertRejected(Events.failed("f2", "r5", "p", 1, 40L, false, "late"),
                RejectReason.TERMINAL_RUN);
    }

    @Test
    void staleTimestampsAreRejected() {
        ingest.ingest(Events.scheduled("s1", "r6", "p", 100L));
        assertRejected(Events.started("t1", "r6", "p", 1, 90L),
                RejectReason.STALE_TIMESTAMP);
        assertTrue(ingest.ingest(Events.started("t1b", "r6", "p", 1, 110L)).accepted());
        assertRejected(Events.succeeded("o1", "r6", "p", 1, 105L),
                RejectReason.STALE_TIMESTAMP);
    }

    @Test
    void failedEventRequiresReason() {
        ingest.ingest(Events.scheduled("s1", "r7", "p", 0L));
        ingest.ingest(Events.started("t1", "r7", "p", 1, 10L));
        assertRejected(Events.failed("f0", "r7", "p", 1, 20L, true, "  "),
                RejectReason.MISSING_FAILURE_REASON);
    }

    @Test
    void retryChainTransitionsAndMaxAttempts() {
        String run = "r8";
        ingest.ingest(Events.scheduled("s", run, "p", 0L));
        ingest.ingest(Events.started("t1", run, "p", 1, 10L));
        IngestResult f1 = ingest.ingest(Events.failed("f1", run, "p", 1, 20L, true, "boom"));
        assertTrue(f1.accepted());
        assertEquals(RunState.AWAITING_RETRY, f1.runState(), "可重试失败应进入 AWAITING_RETRY");

        // 第 2 次尝试：不可重试失败 -> FAILED_FINAL
        assertTrue(ingest.ingest(Events.started("t2", run, "p", 2, 30L)).accepted());
        IngestResult f2 = ingest.ingest(
                Events.failed("f2", run, "p", 2, 40L, false, "fatal"));
        assertEquals(RunState.FAILED_FINAL, f2.runState(), "不可重试失败为终态失败");
        System.out.println("[重试链] f1 依据: " + f1.message());
        System.out.println("[重试链] f2 依据: " + f2.message());

        // 终态后继续重试必须拒绝
        assertRejected(Events.started("t3", run, "p", 3, 50L),
                RejectReason.TERMINAL_RUN);
    }

    @Test
    void retriesExhaustingMaxAttemptsBecomeFinalFailure() {
        String run = "r9";
        ingest.ingest(Events.scheduled("s", run, "p", 0L));
        ingest.ingest(Events.started("t1", run, "p", 1, 10L));
        ingest.ingest(Events.failed("f1", run, "p", 1, 20L, true, "e1"));
        ingest.ingest(Events.started("t2", run, "p", 2, 30L));
        ingest.ingest(Events.failed("f2", run, "p", 2, 40L, true, "e2"));
        ingest.ingest(Events.started("t3", run, "p", 3, 50L));
        IngestResult f3 = ingest.ingest(
                Events.failed("f3", run, "p", 3, 60L, true, "e3"));
        assertEquals(RunState.FAILED_FINAL, f3.runState(),
                "第 3 次可重试失败但已达上限，必须终态失败");
        System.out.println("[重试耗尽] 依据: " + f3.message());
        // 第 4 次 STARTED 超过 maxAttempts，直接按非法流转拒绝（此时已终态）
        assertRejected(Events.started("t4", run, "p", 4, 70L),
                RejectReason.TERMINAL_RUN);
    }

    private static String brief(PipelineEvent e) {
        return e.eventId() + "/" + e.runId() + ":" + e.type()
                + "(attempt=" + e.attempt() + ",time=" + e.eventTime()
                + (e.type() == com.github.highcumontoa.pipelineslaobservabilityjava.model.EventType.FAILED
                ? ",retryable=" + e.retryable() : "") + ")";
    }
}
