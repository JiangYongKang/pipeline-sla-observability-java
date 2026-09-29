package com.github.highcumontoa.pipelineslaobservabilityjava.service;

/**
 * 单个事件接入结果。
 *
 * @param accepted  是否被接受
 * @param eventId   事件 ID
 * @param runId     运行 ID
 * @param duplicate 是否为幂等重复(同一 eventId 且载荷一致)
 * @param reason    被拒绝时的结构化原因名(接受时为 null)
 */
public record IngestResult(boolean accepted, String eventId, String runId,
                           boolean duplicate, String reason) {

    public static IngestResult accepted(String eventId, String runId) {
        return new IngestResult(true, eventId, runId, false, null);
    }

    public static IngestResult duplicate(String eventId, String runId) {
        return new IngestResult(true, eventId, runId, true, null);
    }
}
