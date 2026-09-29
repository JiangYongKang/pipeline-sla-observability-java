package com.github.highcumontoa.pipelineslaobservabilityjava.domain;

/**
 * 事件接入被拒绝时抛出, 携带结构化拒绝原因, 便于调用方按原因区分。
 */
public class EventRejectedException extends RuntimeException {

    private final RejectReason reason;
    private final String eventId;
    private final String runId;

    public EventRejectedException(RejectReason reason, String runId, String eventId, String detail) {
        super("event rejected: reason=" + reason + ", runId=" + runId + ", eventId=" + eventId
                + (detail == null ? "" : ", detail=" + detail));
        this.reason = reason;
        this.eventId = eventId;
        this.runId = runId;
    }

    public RejectReason getReason() {
        return reason;
    }

    public String getEventId() {
        return eventId;
    }

    public String getRunId() {
        return runId;
    }
}
