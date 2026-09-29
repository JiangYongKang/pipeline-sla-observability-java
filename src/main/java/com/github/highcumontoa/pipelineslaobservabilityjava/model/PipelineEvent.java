package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/**
 * 流水线运行事件（接入层输入）。
 *
 * @param eventId      事件唯一 ID，用于重复提交去重
 * @param runId        一次调度运行的唯一 ID（重试共享同一 runId）
 * @param pipelineName 流水线名称
 * @param type         事件类型
 * @param attempt      尝试序号，从 1 开始
 * @param eventTime    事件发生时间（epoch millis，来自产生端）
 * @param retryable    仅 FAILED 使用：该失败是否可重试
 * @param failureReason 仅 FAILED 使用：失败原因
 * @param itemCount    本次尝试处理的条目数（用于吞吐聚合，可为 0）
 */
public record PipelineEvent(
        String eventId,
        String runId,
        String pipelineName,
        EventType type,
        int attempt,
        long eventTime,
        Boolean retryable,
        String failureReason,
        Long itemCount
) {
}
