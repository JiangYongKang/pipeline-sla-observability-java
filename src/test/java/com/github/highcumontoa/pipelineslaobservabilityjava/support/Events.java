package com.github.highcumontoa.pipelineslaobservabilityjava.support;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.EventType;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.PipelineEvent;

/** 测试事件构造简写。 */
public final class Events {
    private Events() {
    }

    public static PipelineEvent scheduled(String eventId, String runId, String pipeline, long t) {
        return new PipelineEvent(eventId, runId, pipeline, EventType.SCHEDULED,
                0, t, null, null, null);
    }

    public static PipelineEvent started(String eventId, String runId, String pipeline,
                                        int attempt, long t) {
        return new PipelineEvent(eventId, runId, pipeline, EventType.STARTED,
                attempt, t, null, null, 0L);
    }

    public static PipelineEvent started(String eventId, String runId, String pipeline,
                                        int attempt, long t, long items) {
        return new PipelineEvent(eventId, runId, pipeline, EventType.STARTED,
                attempt, t, null, null, items);
    }

    public static PipelineEvent succeeded(String eventId, String runId, String pipeline,
                                          int attempt, long t) {
        return new PipelineEvent(eventId, runId, pipeline, EventType.SUCCEEDED,
                attempt, t, null, null, 0L);
    }

    public static PipelineEvent succeeded(String eventId, String runId, String pipeline,
                                          int attempt, long t, long items) {
        return new PipelineEvent(eventId, runId, pipeline, EventType.SUCCEEDED,
                attempt, t, null, null, items);
    }

    public static PipelineEvent failed(String eventId, String runId, String pipeline,
                                       int attempt, long t, boolean retryable, String reason) {
        return new PipelineEvent(eventId, runId, pipeline, EventType.FAILED,
                attempt, t, retryable, reason, 0L);
    }
}
