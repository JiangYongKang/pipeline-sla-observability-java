package com.github.highcumontoa.pipelineslaobservabilityjava.model;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一次运行的完整可变状态, 是事件接入、SLA 判定与指标聚合的唯一事实来源。
 */
public class RunState {
    public String pipelineId;
    public String runId;
    public RunStatus status;
    public Instant scheduledTime;
    public Instant startedAt;
    public Instant finishedAt;
    public long slaMillis;
    public long maxSeq;
    public Instant lastEventAt;
    public String terminalErrorCode;

    /** attemptSeq -> 尝试状态。LinkedHashMap 保证快照/遍历顺序稳定。 */
    public Map<Long, AttemptState> attempts = new LinkedHashMap<>();

    /** 已接收的事件 ID 及对应序号, 用于幂等与冲突检测。 */
    public Map<String, Long> acceptedEventIds = new LinkedHashMap<>();

    /** 乱序暂存: 因序号缺口被拦下的事件, 等待缺口补齐后按序重放。 */
    public List<com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent> pending =
            new ArrayList<>();

    public RunState() {
    }
}
