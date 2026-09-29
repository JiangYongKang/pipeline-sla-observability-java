package com.github.highcumontoa.pipelineslaobservabilityjava.domain;

import java.time.Instant;

/**
 * 流水线运行事件(接入侧输入)。
 *
 * <p>所有判定以事件自身携带的业务时间({@code occurredAt})为准, 而不是服务端接收时刻,
 * 因此迟到/抖动不改变结论。
 *
 * @param eventId       事件唯一 ID, 用于全局幂等去重
 * @param pipelineId    流水线 ID
 * @param runId         运行 ID(同一流水线的一次调度运行)
 * @param seq           该 run 内单调递增的事件序号(从 1 开始), 用于识别乱序
 * @param type          事件类型
 * @param occurredAt    事件业务发生时间
 * @param scheduledTime 本次运行的计划开始时间(RUN_STARTED 必填)
 * @param slaMillis     本次运行的 SLA 时长(RUN_STARTED 必填, 毫秒)
 * @param attemptSeq    尝试序号(ATTEMPT_* 事件必填, 从 1 开始)
 * @param processedCount 该事件累计处理的数据条数(可空, 用于吞吐聚合)
 * @param errorCode     失败错误码(失败类事件可空)
 */
public record PipelineEvent(
        String eventId,
        String pipelineId,
        String runId,
        Long seq,
        EventType type,
        Instant occurredAt,
        Instant scheduledTime,
        Long slaMillis,
        Long attemptSeq,
        Long processedCount,
        String errorCode
) {
}
