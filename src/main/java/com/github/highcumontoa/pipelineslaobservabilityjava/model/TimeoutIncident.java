package com.github.highcumontoa.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 一次超时事件(incident): 超时只会被开启一次, 结束时关闭一次,
 * 保证迟到/抖动的完成事件不会造成重复计数。
 */
public class TimeoutIncident {
    public String pipelineId;
    public String runId;
    public Instant deadline;
    public Instant openedAt;
    public Instant closedAt;
    /** 关闭时的终态判定: true=超时成立(BREACHED); false=deadline 前已结束, 事件仅为延迟到达。 */
    public boolean breached;

    public TimeoutIncident() {
    }
}
