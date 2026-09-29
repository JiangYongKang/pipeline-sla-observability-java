package com.github.highcumontoa.pipelineslaobservabilityjava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 一次运行的本地可变状态：归约所有<b>已接受</b>事件得到。
 * 通过 {@code synchronized(this)} 在存储层保证并发一致性。
 */
public class RunRecord {
    private String runId;
    private String pipelineName;
    private long scheduledAt;
    private Long firstStartedAt;
    private Long finishedAt;
    private RunState state = RunState.SCHEDULED;
    private final List<Attempt> attempts = new ArrayList<>();
    /** 已接受事件 ID 的有序列表（既是去重索引，也是快照恢复来源）。 */
    private final List<String> acceptedEventIds = new ArrayList<>();

    public RunRecord() {
    }

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public long getScheduledAt() { return scheduledAt; }
    public void setScheduledAt(long scheduledAt) { this.scheduledAt = scheduledAt; }
    public Long getFirstStartedAt() { return firstStartedAt; }
    public void setFirstStartedAt(Long firstStartedAt) { this.firstStartedAt = firstStartedAt; }
    public Long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Long finishedAt) { this.finishedAt = finishedAt; }
    public RunState getState() { return state; }
    public void setState(RunState state) { this.state = state; }
    public List<Attempt> getAttempts() { return attempts; }
    public List<String> getAcceptedEventIds() { return acceptedEventIds; }
}
