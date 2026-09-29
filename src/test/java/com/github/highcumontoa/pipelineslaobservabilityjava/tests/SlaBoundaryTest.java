package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaVerdict;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunQueryService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaPolicyRegistry;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import com.github.highcumontoa.pipelineslaobservabilityjava.support.Events;
import com.github.highcumontoa.pipelineslaobservabilityjava.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SLA 边界情形测试：
 * deadline = scheduledAt + timeoutMillis（这里取 1000ms）。
 * 覆盖：恰好边界、超时未完成、终态失败、迟到成功不回退 ON_TIME、重复评估幂等。
 */
class SlaBoundaryTest {

    private static final long TIMEOUT = 1000L;

    private MutableClock clock;
    private IngestService ingest;
    private SlaService sla;
    private RunQueryService query;

    @BeforeEach
    void setUp(@TempDir Path tmp) {
        EventStore store = new EventStore(new ObjectMapper(), tmp.resolve("sla.jsonl").toString());
        SlaPolicyRegistry registry = new SlaPolicyRegistry(TIMEOUT, 3);
        clock = new MutableClock(0L);
        ingest = new IngestService(store, registry);
        sla = new SlaService(store, registry, clock);
        query = new RunQueryService(store);
    }

    private void print(String runId) {
        SlaVerdict v = sla.evaluate(runId);
        System.out.println("[SLA 输入] now=" + clock.nowMillis() + " run=" + runId
                + " state=" + query.get(runId).state());
        System.out.println("[SLA 判定] status=" + v.status() + " deadline=" + v.deadline()
                + " successAt=" + v.successAt() + " delay=" + v.delayMillis()
                + " timedOut=" + v.timedOut() + " recovered=" + v.recovered());
        System.out.println("[判定依据] " + v.basis());
    }

    @Test
    void successExactlyAtDeadlineIsOnTime() {
        String run = "r-boundary";
        ingest.ingest(Events.scheduled("s", run, "p", 0L));
        ingest.ingest(Events.started("t", run, "p", 1, 0L));
        // 恰在 deadline(=1000) 成功，边界含等号 -> ON_TIME
        ingest.ingest(Events.succeeded("o", run, "p", 1, TIMEOUT));
        clock.set(TIMEOUT + 5_000L); // 即使评估时刻很晚，结论只由成功时间决定
        print(run);
        SlaVerdict v = sla.evaluate(run);
        assertEquals(SlaStatus.ON_TIME, v.status(), "成功时间==deadline 应判 ON_TIME");
        assertFalse(v.timedOut());
        assertEquals(0L, v.delayMillis());
    }

    @Test
    void successOneMillisAfterDeadlineIsLateSuccess() {
        String run = "r-late";
        ingest.ingest(Events.scheduled("s", run, "p", 0L));
        ingest.ingest(Events.started("t", run, "p", 1, 0L));
        ingest.ingest(Events.succeeded("o", run, "p", 1, TIMEOUT + 1));
        print(run);
        SlaVerdict v = sla.evaluate(run);
        assertEquals(SlaStatus.LATE_SUCCESS, v.status());
        assertTrue(v.timedOut());
        assertTrue(v.recovered());
        assertEquals(1L, v.delayMillis());
    }

    @Test
    void runningAfterDeadlineTimesOutAndLateSuccessDoesNotFlipBack() {
        String run = "r-jitter";
        ingest.ingest(Events.scheduled("s", run, "p", 0L));
        ingest.ingest(Events.started("t", run, "p", 1, 100L));

        // 截止前：ON_TRACK
        clock.set(TIMEOUT - 1);
        assertEquals(SlaStatus.ON_TRACK, sla.evaluate(run).status());
        System.out.println("[边界-1ms] " + sla.evaluate(run).basis());

        // 截止后仍在跑：TIMEOUT
        clock.set(TIMEOUT + 1);
        SlaVerdict timeout = sla.evaluate(run);
        print(run);
        assertEquals(SlaStatus.TIMEOUT, timeout.status());
        assertTrue(timeout.timedOut());

        // 重复评估幂等：不因“短暂抖动”重复计数或改变结论
        SlaVerdict again = sla.evaluate(run);
        assertEquals(timeout.status(), again.status());
        assertEquals(timeout.basis().startsWith("已过截止时间"),
                again.basis().startsWith("已过截止时间"));

        // 迟到的 SUCCEEDED：只收敛为 LATE_SUCCESS，绝不回退 ON_TIME
        ingest.ingest(Events.succeeded("o-late", run, "p", 1, TIMEOUT + 500));
        print(run);
        SlaVerdict late = sla.evaluate(run);
        assertEquals(SlaStatus.LATE_SUCCESS, late.status());
        assertTrue(late.timedOut(), "超时事实必须保留");
        assertTrue(late.recovered(), "迟到成功计为恢复");

        // 再次重复评估结论稳定
        assertEquals(SlaStatus.LATE_SUCCESS, sla.evaluate(run).status());
    }

    @Test
    void finalFailureBeforeDeadlineIsNotYetTimeoutThenBecomesTimeout() {
        String run = "r-fail";
        ingest.ingest(Events.scheduled("s", run, "p", 0L));
        ingest.ingest(Events.started("t", run, "p", 1, 10L));
        ingest.ingest(Events.failed("f", run, "p", 1, 20L, false, "fatal"));

        clock.set(500L);
        print(run);
        assertEquals(SlaStatus.ON_TRACK, sla.evaluate(run).status(),
                "deadline 前的终态失败仍是 ON_TRACK（尚未超时），状态由 FAILED_FINAL 表达失败");

        clock.set(TIMEOUT + 1);
        print(run);
        SlaVerdict v = sla.evaluate(run);
        assertEquals(SlaStatus.TIMEOUT, v.status());
        assertFalse(v.recovered(), "终态失败不可恢复");
    }

    @Test
    void awaitingRetryAfterDeadlineIsTimeoutAndRetriedSuccessIsLate() {
        String run = "r-retry-sla";
        ingest.ingest(Events.scheduled("s", run, "p", 0L));
        ingest.ingest(Events.started("t1", run, "p", 1, 10L));
        ingest.ingest(Events.failed("f1", run, "p", 1, 200L, true, "transient"));
        clock.set(TIMEOUT + 1);
        assertEquals(SlaStatus.TIMEOUT, sla.evaluate(run).status());
        System.out.println("[等待重试超时] " + sla.evaluate(run).basis());

        // deadline 后重试成功：LATE_SUCCESS
        ingest.ingest(Events.started("t2", run, "p", 2, TIMEOUT + 100));
        ingest.ingest(Events.succeeded("o2", run, "p", 2, TIMEOUT + 400));
        print(run);
        assertEquals(SlaStatus.LATE_SUCCESS, sla.evaluate(run).status());
    }
}
