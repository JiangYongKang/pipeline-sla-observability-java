package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.EventIngestionService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * 同一个运行在多线程并发重复提交下, 最终状态必须一致:
 * 每个 eventId 恰好被接受一次, 其余为幂等重复, run 最终唯一且 SUCCEEDED。
 */
class ConcurrentIngestionTest {

    private static final Instant T0 = Instant.parse("2026-09-29T00:00:00Z");

    @Test
    void concurrentDuplicateSubmissionConvergesToSingleConsistentState() throws Exception {
        int threads = 16;
        Path snapshot = Files.createTempFile("snapshot-concurrent-", ".json");
        Files.deleteIfExists(snapshot);

        Clock clock = Clock.fixed(T0.plusSeconds(3600), ZoneOffset.UTC);
        RunStore store = new RunStore(new ObjectMapper(), snapshot.toString());
        SlaService sla = new SlaService(clock, store);
        EventIngestionService ingestion = new EventIngestionService(store, sla);
        MetricsService metrics = new MetricsService(store, sla, clock);

        // 四个事件构成完整生命周期, 所有线程提交完全相同的事件
        List<PipelineEvent> shared = List.of(
                Events.runStarted("e1", "pipe", "run-x", 1, T0, T0, 60_000),
                Events.attemptStarted("e2", "pipe", "run-x", 2, T0.plusMillis(100), 1),
                Events.attemptSucceeded("e3", "pipe", "run-x", 3, T0.plusMillis(5_000), 1, 100),
                Events.runSucceeded("e4", "pipe", "run-x", 4, T0.plusMillis(5_000))
        );

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        // 每阶段一个共享屏障, 让所有线程在同一时刻并发提交同一个事件
        CyclicBarrier[] phaseBarriers = new CyclicBarrier[shared.size() + 1];
        for (int i = 0; i < phaseBarriers.length; i++) {
            phaseBarriers[i] = new CyclicBarrier(threads);
        }
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    for (int phase = 0; phase < shared.size(); phase++) {
                        phaseBarriers[phase].await(10, TimeUnit.SECONDS);
                        IngestResult r = ingestion.ingest(shared.get(phase));
                        if (r.duplicate()) {
                            duplicate.incrementAndGet();
                        } else if (r.accepted()) {
                            accepted.incrementAndGet();
                        }
                    }
                    phaseBarriers[shared.size()].await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    rejected.incrementAndGet();
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "所有线程应结束");

        System.out.println("[输入] " + threads + " 线程 x " + shared.size()
                + " 个相同事件 = " + (threads * shared.size()) + " 次提交");
        System.out.println("[判定依据] 全局 eventId 幂等 + 单把全局锁串行化状态机");
        System.out.println("[结果] accepted=" + accepted.get() + " duplicate=" + duplicate.get()
                + " rejected=" + rejected.get());

        assertEquals(4, accepted.get(), "每个 eventId 恰好被接受一次");
        assertEquals(threads * 4 - 4, duplicate.get(), "其余必须全部是幂等重复");
        assertEquals(0, rejected.get(), "不允许出现原因不明的拒绝");

        RunView view = metrics.getRun("run-x");
        System.out.println("[最终状态] status=" + view.status() + " attempts=" + view.attemptCount()
                + " sla=" + view.slaStatus());
        assertEquals(RunStatus.SUCCEEDED, view.status());
        assertEquals(1, view.attemptCount());
        assertEquals(0, view.retriableFailureCount());
        assertEquals(0, ingestion.pendingCount("run-x"), "不得残留暂存事件");

        // 用快照重建一个 store, 结论必须一致(无损坏、可恢复)
        RunStore restored = new RunStore(new ObjectMapper(), snapshot.toString());
        SlaService sla2 = new SlaService(clock, restored);
        MetricsService metrics2 = new MetricsService(restored, sla2, clock);
        RunView restoredView = metrics2.getRun("run-x");
        System.out.println("[快照恢复] status=" + restoredView.status()
                + " attempts=" + restoredView.attemptCount() + " sla=" + restoredView.slaStatus());
        assertEquals(view.status(), restoredView.status());
        assertEquals(view.attemptCount(), restoredView.attemptCount());
        assertEquals(view.slaStatus(), restoredView.slaStatus());

        Files.deleteIfExists(snapshot);
    }
}
