package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.EventType;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import java.time.Instant;

/** 测试用事件构造器, 让测试输入事件一目了然。 */
public final class Events {

    private Events() {
    }

    public static PipelineEvent runStarted(String eventId, String pipelineId, String runId,
                                           long seq, Instant occurredAt,
                                           Instant scheduledAt, long slaMillis) {
        return new PipelineEvent(eventId, pipelineId, runId, seq, EventType.RUN_STARTED,
                occurredAt, scheduledAt, slaMillis, null, null, null);
    }

    public static PipelineEvent attemptStarted(String eventId, String pipelineId, String runId,
                                               long seq, Instant occurredAt, long attemptSeq) {
        return new PipelineEvent(eventId, pipelineId, runId, seq, EventType.ATTEMPT_STARTED,
                occurredAt, null, null, attemptSeq, null, null);
    }

    public static PipelineEvent attemptFailure(String eventId, String pipelineId, String runId,
                                               long seq, Instant occurredAt, long attemptSeq,
                                               String errorCode) {
        return new PipelineEvent(eventId, pipelineId, runId, seq,
                EventType.ATTEMPT_RETRIABLE_FAILURE, occurredAt, null, null,
                attemptSeq, null, errorCode);
    }

    public static PipelineEvent attemptFailure(String eventId, String pipelineId, String runId,
                                               long seq, Instant occurredAt, long attemptSeq,
                                               String errorCode, long processedCount) {
        return new PipelineEvent(eventId, pipelineId, runId, seq,
                EventType.ATTEMPT_RETRIABLE_FAILURE, occurredAt, null, null,
                attemptSeq, processedCount, errorCode);
    }

    public static PipelineEvent attemptSucceeded(String eventId, String pipelineId, String runId,
                                                 long seq, Instant occurredAt,
                                                 long attemptSeq, long processedCount) {
        return new PipelineEvent(eventId, pipelineId, runId, seq, EventType.ATTEMPT_SUCCEEDED,
                occurredAt, null, null, attemptSeq, processedCount, null);
    }

    public static PipelineEvent runSucceeded(String eventId, String pipelineId, String runId,
                                             long seq, Instant occurredAt) {
        return new PipelineEvent(eventId, pipelineId, runId, seq, EventType.RUN_SUCCEEDED,
                occurredAt, null, null, null, null, null);
    }

    public static PipelineEvent runFailed(String eventId, String pipelineId, String runId,
                                          long seq, Instant occurredAt, String errorCode) {
        return new PipelineEvent(eventId, pipelineId, runId, seq, EventType.RUN_FAILED,
                occurredAt, null, null, null, null, errorCode);
    }
}
