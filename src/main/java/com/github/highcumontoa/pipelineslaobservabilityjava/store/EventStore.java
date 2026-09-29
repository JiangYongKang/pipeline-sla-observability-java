package com.github.highcumontoa.pipelineslaobservabilityjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地事件与运行状态存储。
 *
 * <p>一致性模型：
 * <ul>
 *   <li>所有读写都在同一把内置锁上串行化，因此多线程并发提交同一 runId 时，
 *       事件按获得锁的总顺序归约，最终状态与“某一次串行提交顺序”完全一致；</li>
 *   <li>已接受事件以 append-only 方式写入本地 JSONL 快照文件（fsync），
 *       只有落盘成功才对内存可见，避免“已接受但丢失”；</li>
 *   <li>重启时按快照中的接受顺序重放事件重建 RunRecord，结论与宕机前一致。</li>
 * </ul>
 */
@Component
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private final Map<String, RunRecord> records = new LinkedHashMap<>();
    /** eventId -> runId，重复提交检测的全局索引。 */
    private final Map<String, String> eventIndex = new LinkedHashMap<>();
    /** 已接受事件，严格保持接受顺序（也是快照重放顺序）。 */
    private final List<PipelineEvent> acceptedEvents = new ArrayList<>();

    private final ObjectMapper objectMapper;
    private final Path snapshotPath;

    public EventStore(ObjectMapper objectMapper,
                      @Value("${pipeline.snapshot.path:data/events.jsonl}") String snapshotFile) {
        this.objectMapper = objectMapper;
        this.snapshotPath = Path.of(snapshotFile);
    }

    /**
     * 原子地登记一个已接受事件：更新去重索引、运行记录并追加持久化。
     * 调用方（状态机）必须保证事件合法；存储层只负责原子可见性与持久化。
     */
    public synchronized void append(PipelineEvent event, RunRecord record) {
        records.put(record.getRunId(), record);
        eventIndex.put(event.eventId(), event.runId());
        acceptedEvents.add(event);
        persist(event);
    }

    /** 快照重放期间使用：只登记内存状态，不再次落盘。 */
    public synchronized void putRecord(RunRecord record) {
        records.put(record.getRunId(), record);
    }

    /** 快照重放期间使用：登记事件与去重索引，不落盘。 */
    public synchronized void indexAcceptedEvent(PipelineEvent event) {
        eventIndex.put(event.eventId(), event.runId());
        acceptedEvents.add(event);
    }

    public synchronized boolean isKnownEvent(String eventId) {
        return eventIndex.containsKey(eventId);
    }

    public synchronized String runOfEvent(String eventId) {
        return eventIndex.get(eventId);
    }

    public synchronized RunRecord getRecord(String runId) {
        return records.get(runId);
    }

    public synchronized List<RunRecord> allRecords() {
        return new ArrayList<>(records.values());
    }

    public synchronized List<PipelineEvent> acceptedEvents() {
        return new ArrayList<>(acceptedEvents);
    }

    private void persist(PipelineEvent event) {
        try {
            Files.createDirectories(snapshotPath.getParent() != null
                    ? snapshotPath.getParent() : Path.of("."));
            String line = objectMapper.writeValueAsString(event) + System.lineSeparator();
            Files.writeString(snapshotPath, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("事件持久化失败: " + event.eventId(), e);
        }
    }

    private final AtomicBoolean recovered = new AtomicBoolean(false);

    /**
     * 从本地快照按接受顺序重放，恢复运行状态与去重索引。
     * 由状态机（IngestService）在自身构造完成后触发；AtomicBoolean 保证只执行一次。
     */
    public void recoverIfNeeded(EventReplayer replayer) {
        if (!recovered.compareAndSet(false, true)) {
            return;
        }
        if (!Files.exists(snapshotPath)) {
            log.info("未发现本地快照 {}，以空存储启动", snapshotPath.toAbsolutePath());
            return;
        }
        long count;
        try {
            List<String> lines = Files.readAllLines(snapshotPath, StandardCharsets.UTF_8);
            count = lines.stream()
                    .filter(l -> !l.isBlank())
                    .map(this::parse)
                    .peek(replayer::replay)
                    .count();
        } catch (IOException e) {
            throw new IllegalStateException("读取本地快照失败: " + snapshotPath, e);
        }
        log.info("从本地快照 {} 重放 {} 个已接受事件完成", snapshotPath.toAbsolutePath(), count);
    }

    private PipelineEvent parse(String line) {
        try {
            return objectMapper.readValue(line, PipelineEvent.class);
        } catch (IOException e) {
            throw new IllegalStateException("快照行无法解析: " + line, e);
        }
    }

    /** 快照恢复重放端口：由接入状态机实现。 */
    public interface EventReplayer {
        /** 重放一个曾经被接受的事件（重建内存状态，不再持久化）。 */
        void replay(PipelineEvent event);
    }
}
