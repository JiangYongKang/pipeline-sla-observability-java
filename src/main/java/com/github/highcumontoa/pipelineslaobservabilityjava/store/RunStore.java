package com.github.highcumontoa.pipelineslaobservabilityjava.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.TimeoutIncident;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 本地存储: 内存保存所有运行状态与超时事件, 并通过快照文件持久化。
 *
 * <p>并发模型: 用一把全局可重入锁串行化所有状态变更(含快照写入),
 * 保证同一运行在多线程并发提交下最终状态一致、快照不会出现撕裂内容。
 * 可重入意味着一次接入流程内部嵌套的读取/写入不会自锁。
 *
 * <p>恢复模型: 保存的是事件应用后的<b>最终状态</b>(而非原始事件), 因此服务重启后
 * 查询与聚合结果与重启前完全一致。
 */
@Component
public class RunStore {

    /** 快照文件结构。 */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class Snapshot {
        public Map<String, RunState> runs = new TreeMap<>();
        public Map<String, TimeoutIncident> incidents = new TreeMap<>();
    }

    private final ObjectMapper objectMapper;
    private final Path snapshotPath;

    private final Map<String, RunState> runs = new TreeMap<>();
    private final Map<String, TimeoutIncident> incidents = new TreeMap<>();
    /** 全局事件 ID -> runId, 跨运行幂等去重。可从各 run 的 acceptedEventIds 重建, 无需单独入快照。 */
    private final Map<String, String> globalEventIds = new TreeMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public RunStore(ObjectMapper objectMapper,
                    @Value("${pipeline.snapshot.path:}") String snapshotPath) {
        this.objectMapper = objectMapper.copy();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.snapshotPath = (snapshotPath == null || snapshotPath.isBlank()) ? null : Path.of(snapshotPath);
        loadSnapshot();
    }

    private void loadSnapshot() {
        if (snapshotPath == null || !Files.exists(snapshotPath)) {
            return;
        }
        try {
            if (Files.size(snapshotPath) == 0) {
                return;
            }
            Snapshot snapshot = objectMapper.readValue(snapshotPath.toFile(), Snapshot.class);
            if (snapshot.runs != null) {
                runs.putAll(snapshot.runs);
            }
            if (snapshot.incidents != null) {
                incidents.putAll(snapshot.incidents);
            }
            // 从各运行重建全局事件 ID 索引
            runs.values().forEach(r -> r.acceptedEventIds
                    .forEach((eventId, seq) -> globalEventIds.put(eventId, r.runId)));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load snapshot from " + snapshotPath, e);
        }
    }

    /** 在全局锁下执行一个读或写动作, 返回其结果。 */
    public <T> T withLock(Supplier<T> action) {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    /** 在全局锁下执行无返回值动作。 */
    public void withLock(Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    public RunState findRun(String runId) {
        return runs.get(runId);
    }

    public RunState createRun(RunState state) {
        RunState previous = runs.putIfAbsent(state.runId, state);
        if (previous != null) {
            return previous;
        }
        return state;
    }

    public Collection<RunState> allRuns() {
        return runs.values();
    }

    public TimeoutIncident incidentFor(String runId) {
        return incidents.get(runId);
    }

    public void putIncident(TimeoutIncident incident) {
        incidents.put(incident.runId, incident);
    }

    public Collection<TimeoutIncident> allIncidents() {
        return incidents.values();
    }

    /** 返回 eventId 此前归属的 runId; 从未出现过则 null。 */
    public String globalEventIdRun(String eventId) {
        return globalEventIds.get(eventId);
    }

    /** 登记全局事件 ID 与 runId 的归属。 */
    public void registerGlobalEventId(String eventId, String runId) {
        globalEventIds.put(eventId, runId);
    }

    /**
     * 将当前全量状态原子写入快照文件(调用方必须已持有锁)。
     * 先写临时文件再原子移动, 避免崩溃留下半文件。
     */
    public void persistLocked() {
        if (snapshotPath == null) {
            return;
        }
        try {
            Snapshot snapshot = new Snapshot();
            snapshot.runs.putAll(runs);
            snapshot.incidents.putAll(incidents);
            Path parent = snapshotPath.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = snapshotPath.resolveSibling(snapshotPath.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, snapshotPath, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist snapshot to " + snapshotPath, e);
        }
    }
}
