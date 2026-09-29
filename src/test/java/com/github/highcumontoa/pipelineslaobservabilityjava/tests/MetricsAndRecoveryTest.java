package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.AggregationResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunQueryService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaPolicyRegistry;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import com.github.highcumontoa.pipelineslaobservabilityjava.support.Events;
import com.github.highcumontoa.pipelineslaobservabilityjava.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合稳定性测试：
 * 1) 同一批事件以不同到达顺序接入，聚合结果必须相同；
 * 2) 服务重建并从本地快照重放后，运行状态与聚合结果必须保持稳定。
 */
class MetricsAndRecoveryTest {

    private static final long TIMEOUT = 1000L;

    private record Stack(EventStore store, IngestService ingest, MetricsService metrics,
                         RunQueryService query, MutableClock clock) {
    }

    private Stack newStack(Path tmp) {
        EventStore store = new EventStore(new ObjectMapper(), tmp.resolve("m.jsonl").toString());
        SlaPolicyRegistry registry = new SlaPolicyRegistry(TIMEOUT, 3);
        MutableClock clock = new MutableClock(5_000L);
        IngestService ingest = new IngestService(store, registry);
        SlaService sla = new SlaService(store, registry, clock);
        MetricsService metrics = new MetricsService(store, registry, sla, clock);
        RunQueryService query = new RunQueryService(store);
        return new Stack(store, ingest, metrics, query, clock);
    }

    /** 构造 3 个运行：1 个一次成功(ON_TIME)、1 个重试后成功但迟到(LATE_SUCCESS)、1 个终态失败(TIMEOUT)。 */
    private List<PipelineEvent> scenario() {
        List<PipelineEvent> ev = new ArrayList<>();
        // run-a: 0~100 成功，吞吐 100 items / 100ms = 1000 items/s
        ev.add(Events.scheduled("a-s", "run-a", "pipe", 0L));
        ev.add(Events.started("a-t", "run-a", "pipe", 1, 0L, 100L));
        ev.add(Events.succeeded("a-o", "run-a", "pipe", 1, 100L, 100L));
        // run-b: 首次失败(可重试)，第 2 次在 deadline 后成功
        ev.add(Events.scheduled("b-s", "run-b", "pipe", 0L));
        ev.add(Events.started("b-t1", "run-b", "pipe", 1, 10L, 0L));
        ev.add(Events.failed("b-f1", "run-b", "pipe", 1, 200L, true, "transient"));
        ev.add(Events.started("b-t2", "run-b", "pipe", 2, 1100L, 300L));
        ev.add(Events.succeeded("b-o2", "run-b", "pipe", 2, 1300L, 300L));
        // run-c: 不可重试终态失败，deadline 后评估
        ev.add(Events.scheduled("c-s", "run-c", "pipe", 0L));
        ev.add(Events.started("c-t", "run-c", "pipe", 1, 10L, 0L));
        ev.add(Events.failed("c-f", "run-c", "pipe", 1, 300L, false, "fatal"));
        return ev;
    }

    @Test
    void aggregationIsIndependentOfArrivalOrder(@TempDir Path tmp) throws Exception {
        List<PipelineEvent> events = scenario();

        Files.createDirectories(tmp.resolve("a"));
        Stack s1 = newStack(tmp.resolve("a"));
        ingestAll(s1.ingest(), events);
        AggregationResult r1 = s1.metrics().aggregate("pipe", 0L, 10_000L);

        Files.createDirectories(tmp.resolve("b"));
        Stack s2 = newStack(tmp.resolve("b"));
        List<PipelineEvent> shuffled = new ArrayList<>(events);
        Collections.reverse(shuffled);
        // 乱序中非法的会被拒绝：按依赖反复重投直到全部合法事件接受，
        // 模拟真实接入端“拒绝后重投”，最终事件集合相同。
        ingestUntilStable(s2.ingest(), shuffled);
        AggregationResult r2 = s2.metrics().aggregate("pipe", 0L, 10_000L);

        System.out.println("[聚合-正序] " + r1);
        System.out.println("[聚合-逆序重投] " + r2);

        assertEquals(r1, r2, "到达顺序不同，聚合结果必须一致");
        // 关键指标断言
        assertEquals(3, r1.totalRuns());
        assertEquals(2, r1.succeededRuns());
        assertEquals(1, r1.finalFailedRuns());
        assertEquals(1, r1.timeoutRuns(), "run-c 终态失败且过 deadline");
        assertEquals(1, r1.lateSuccessRuns(), "run-b 迟到成功");
        assertEquals(1, r1.retryableFailures(), "run-b 首次可重试失败恰好计 1 次");
        assertEquals(4, r1.totalAttempts(), "尝试总数 = 1+2+1，乱序与重投不重复计数");
        assertEquals(4.0 / 3, r1.averageAttemptsPerRun(), 0.01);
        assertTrue(r1.totalRunDurationMillis() > 0);
        assertTrue(r1.totalThroughputItemsPerSec() > 0);
    }

    @Test
    void snapshotRecoveryYieldsSameStateAndAggregation(@TempDir Path tmp) throws Exception {
        Path dir = tmp.resolve("rec");
        Files.createDirectories(dir);
        Stack before = newStack(dir);
        ingestAll(before.ingest(), scenario());
        AggregationResult beforeAgg = before.metrics().aggregate("pipe", 0L, 10_000L);
        Path snapshot = dir.resolve("m.jsonl");
        long lines = Files.readAllLines(snapshot).size();
        System.out.println("[快照] 已接受事件落盘行数=" + lines);

        // 模拟重启：新 store/服务指向同一快照文件，触发恢复重放
        Stack after = newStack(dir);
        after.store().recoverIfNeeded(after.ingest());
        AggregationResult afterAgg = after.metrics().aggregate("pipe", 0L, 10_000L);

        System.out.println("[恢复前聚合] " + beforeAgg);
        System.out.println("[恢复后聚合] " + afterAgg);
        assertEquals(beforeAgg, afterAgg, "快照恢复后聚合必须保持稳定");

        // 运行级状态逐一一致
        for (String runId : List.of("run-a", "run-b", "run-c")) {
            assertEquals(before.query().get(runId), after.query().get(runId),
                    runId + " 恢复后状态视图必须一致");
        }
        assertEquals(RunState.SUCCEEDED, after.query().get("run-a").state());
        assertEquals(RunState.SUCCEEDED, after.query().get("run-b").state());
        assertEquals(RunState.FAILED_FINAL, after.query().get("run-c").state());

        // 恢复后重放已接受事件：必须全部识别为 DUPLICATE_EVENT，不产生第二次落盘
        for (PipelineEvent e : scenario()) {
            var r = after.ingest().ingest(e);
            assertTrue(r.duplicate(), "恢复后重放旧事件必须幂等: " + e.eventId());
            assertNotEquals(null, r.reason());
        }
        long linesAfter = Files.readAllLines(snapshot).size();
        assertEquals(lines, linesAfter, "重复事件不得再次落盘");
        System.out.println("[恢复后重复提交] 快照行数保持 " + linesAfter + "，无重复计数");
    }

    private static void ingestAll(IngestService ingest, List<PipelineEvent> events) {
        for (PipelineEvent e : events) {
            var r = ingest.ingest(e);
            assertTrue(r.accepted(), "正序场景必须全部接受: " + e.eventId() + " " + r.message());
        }
    }

    private static void ingestUntilStable(IngestService ingest, List<PipelineEvent> events) {
        boolean progress = true;
        int rounds = 0;
        while (progress && rounds < 10) {
            progress = false;
            rounds++;
            for (PipelineEvent e : events) {
                var r = ingest.ingest(e);
                if (r.accepted()) {
                    progress = true;
                }
            }
        }
        System.out.println("[乱序重投] 收敛轮数=" + rounds);
    }
}
