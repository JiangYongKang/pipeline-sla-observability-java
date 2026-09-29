package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RejectReason;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunQueryService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaPolicyRegistry;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import com.github.highcumontoa.pipelineslaobservabilityjava.support.Events;
import com.github.highcumontoa.pipelineslaobservabilityjava.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发重复提交测试。
 *
 * <p>区分两类场景：
 * <ul>
 *   <li>重复提交：合法链先串行接受，之后同一批事件多线程重放，必须全部幂等识别；</li>
 *   <li>乱序并发：新事件乱序竞争时，拒绝必须可按原因归类且状态始终自洽，
 *       不同运行之间互不影响、最终状态各自一致。</li>
 * </ul>
 */
class IngestConcurrencyTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private List<Object> newStack(Path tmp) {
        EventStore store = new EventStore(mapper, tmp.resolve("events.jsonl").toString());
        SlaPolicyRegistry registry = new SlaPolicyRegistry(60_000, 3);
        MutableClock clock = new MutableClock(0);
        IngestService ingest = new IngestService(store, registry);
        RunQueryService query = new RunQueryService(store);
        return List.of(store, registry, clock, ingest, query);
    }

    @Test
    void concurrentDuplicateSubmissionIsIdempotent(@TempDir Path tmp) throws Exception {
        var stack = newStack(tmp);
        IngestService ingest = (IngestService) stack.get(3);
        RunQueryService query = (RunQueryService) stack.get(4);

        String runId = "run-dup-1";
        List<PipelineEvent> canonical = List.of(
                Events.scheduled("e-sched", runId, "p", 0L),
                Events.started("e-start", runId, "p", 1, 100L),
                Events.failed("e-fail", runId, "p", 1, 200L, true, "transient"),
                Events.started("e-start-2", runId, "p", 2, 300L));

        // 合法链先串行接受
        for (PipelineEvent e : canonical) {
            IngestResult r = ingest.ingest(e);
            assertTrue(r.accepted(), "初始合法链必须接受: " + describeEvent(e) + " 依据:" + r.message());
        }

        // 同一批事件每个复制 10 份，16 线程并发重复提交并打乱
        List<PipelineEvent> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.addAll(canonical);
        }
        Collections.shuffle(tasks);

        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger duplicates = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        List<String> bases = Collections.synchronizedList(new ArrayList<>());
        for (PipelineEvent e : tasks) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                IngestResult r = ingest.ingest(e);
                if (r.accepted()) {
                    accepted.incrementAndGet();
                } else if (r.duplicate()) {
                    duplicates.incrementAndGet();
                } else {
                    other.incrementAndGet();
                }
                bases.add(describeEvent(e) + " => " + summarize(r));
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        System.out.println("[输入事件] " + describe(canonical));
        System.out.println("[重复并发] 总提交=" + tasks.size() + " accepted=" + accepted.get()
                + " duplicate=" + duplicates.get() + " otherRejected=" + other.get());
        bases.stream().limit(6).forEach(b -> System.out.println("[判定依据] " + b));

        assertEquals(0, accepted.get(), "已接受事件不得因重复提交而二次计数");
        assertEquals(0, other.get(), "重复提交只能识别为 DUPLICATE_EVENT");
        assertEquals(tasks.size(), duplicates.get());

        RunView view = query.get(runId);
        System.out.println("[最终状态] state=" + view.state() + " attempts=" + view.attemptCount());
        assertEquals(RunState.RUNNING, view.state());
        assertEquals(2, view.attemptCount(), "重复提交不改变尝试次数");
        assertEquals(4, ingest.acceptedEventIds(runId).size());
    }

    @Test
    void concurrentOutOfOrderEventsAreClassifiedAndStateStaysConsistent(@TempDir Path tmp)
            throws Exception {
        var stack = newStack(tmp);
        IngestService ingest = (IngestService) stack.get(3);
        RunQueryService query = (RunQueryService) stack.get(4);

        String runId = "run-ooo-1";
        List<PipelineEvent> canonical = List.of(
                Events.scheduled("o-sched", runId, "p", 0L),
                Events.started("o-start", runId, "p", 1, 100L),
                Events.succeeded("o-ok", runId, "p", 1, 200L));
        List<PipelineEvent> tasks = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            tasks.addAll(canonical);
        }
        Collections.shuffle(tasks);

        ExecutorService pool = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        Map<RejectReason, AtomicInteger> reasonCounts = new EnumMap<>(RejectReason.class);
        for (RejectReason r : RejectReason.values()) {
            reasonCounts.put(r, new AtomicInteger());
        }
        AtomicInteger accepted = new AtomicInteger();
        List<String> bases = Collections.synchronizedList(new ArrayList<>());
        for (PipelineEvent e : tasks) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                IngestResult r = ingest.ingest(e);
                if (r.accepted()) {
                    accepted.incrementAndGet();
                } else {
                    reasonCounts.get(r.reason()).incrementAndGet();
                }
                bases.add(describeEvent(e) + " => " + summarize(r));
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        System.out.println("[输入事件] " + describe(canonical) + " 并发打乱重复 "
                + (tasks.size() / canonical.size()) + " 份");
        System.out.println("[判定统计] accepted=" + accepted.get() + " 按原因拒绝="
                + reasonCounts.entrySet().stream()
                .filter(en -> en.getValue().get() > 0)
                .map(en -> en.getKey() + "=" + en.getValue().get()).toList());
        bases.stream().limit(10).forEach(b -> System.out.println("[判定依据] " + b));

        // 不变量：接受数 + 各类拒绝数 = 总提交；接受数只能是 1..3 的合法链
        int totalRejected = reasonCounts.values().stream().mapToInt(AtomicInteger::get).sum();
        assertEquals(tasks.size(), accepted.get() + totalRejected, "所有提交必须有明确结论");
        assertTrue(accepted.get() >= 1 && accepted.get() <= canonical.size(),
                "接受的事件必须构成合法状态链子集");
        assertEquals(accepted.get(), ingest.acceptedEventIds(runId).size(),
                "内存中已接受事件与计数一致，无静默覆盖");

        RunView view = query.get(runId);
        System.out.println("[最终状态] state=" + view.state() + " attempts=" + view.attemptCount());
        // 状态自洽：有 SUCCEEDED 接受则终态成功，否则不可能是 SUCCEEDED
        boolean successAccepted = ingest.acceptedEventIds(runId).contains("o-ok");
        assertEquals(successAccepted, view.state() == RunState.SUCCEEDED,
                "终态只能由被接受的 SUCCEEDED 决定");
    }

    @Test
    void manyRunsSubmittedConcurrentlyEachReachConsistentFinalState(@TempDir Path tmp)
            throws Exception {
        var stack = newStack(tmp);
        IngestService ingest = (IngestService) stack.get(3);
        RunQueryService query = (RunQueryService) stack.get(4);

        int runs = 50;
        List<PipelineEvent> all = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            String id = "run-" + i;
            all.add(Events.scheduled(id + "-s", id, "p", i));
            all.add(Events.started(id + "-t", id, "p", 1, i + 10L));
            all.add(Events.succeeded(id + "-o", id, "p", 1, i + 20L));
        }
        Collections.shuffle(all);

        ExecutorService pool = Executors.newFixedThreadPool(24);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        for (PipelineEvent e : all) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                if (ingest.ingest(e).accepted()) {
                    accepted.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        // 每个运行的 3 个事件中，只有“恰好抢到合法位置”的会被接受；
        // 重放被拒绝的事件直到链完整（真实接入端会重投），最终每个运行必须一致成功。
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < runs; i++) {
                String id = "run-" + i;
                for (PipelineEvent e : List.of(
                        Events.scheduled(id + "-s", id, "p", i),
                        Events.started(id + "-t", id, "p", 1, i + 10L),
                        Events.succeeded(id + "-o", id, "p", 1, i + 20L))) {
                    IngestResult r = ingest.ingest(e);
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    }
                }
            }
        }
        System.out.println("[多运行并发] runs=" + runs + " 最终累计接受事件=" + accepted.get());
        for (int i = 0; i < runs; i++) {
            RunView v = query.get("run-" + i);
            assertEquals(RunState.SUCCEEDED, v.state(), "run-" + i + " 最终必须 SUCCEEDED");
            assertEquals(1, v.attemptCount(), "run-" + i + " 恰好一次尝试");
        }
        assertEquals(runs * 3, accepted.get(),
                "50 个运行 ×3 事件，重投补齐后接受总数必须确定为 150，不重不漏");
    }

    private static String describeEvent(PipelineEvent e) {
        return e.eventId() + ":" + e.type() + "(a" + e.attempt() + ",t=" + e.eventTime() + ")";
    }

    private static String summarize(IngestResult r) {
        return r.accepted()
                ? "ACCEPTED-> " + r.runState()
                : (r.duplicate() ? "DUPLICATE_EVENT" : r.reason().toString())
                + " [" + r.message() + "]";
    }

    private static String describe(List<PipelineEvent> events) {
        return events.stream().map(IngestConcurrencyTest::describeEvent).toList().toString();
    }
}
