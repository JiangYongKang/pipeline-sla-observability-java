package com.github.highcumontoa.pipelineslaobservabilityjava.domain;

/**
 * 事件被拒绝接入的原因, 调用方可据此区分处理, 而不是得到静默成功。
 */
public enum RejectReason {
    /** 事件字段缺失或非法(如时间戳为 null、未知事件类型、scheduleTime 缺失等)。 */
    INVALID_PAYLOAD,
    /** 同一 eventId 已被接收过(幂等重复提交)。 */
    DUPLICATE_EVENT,
    /** 同一 eventId 已出现过, 但本次载荷与首次不一致。 */
    EVENT_ID_CONFLICT,
    /** 目标 run 不存在, 且该事件本身不是 RUN_STARTED。 */
    UNKNOWN_RUN,
    /** 该 runId 已经有过 RUN_STARTED, 不能再次启动。 */
    DUPLICATE_RUN,
    /** 事件类型相对于运行当前生命周期状态构成回退/越权流转。 */
    ILLEGAL_TRANSITION,
    /** 事件序号小于等于该 run 已接收的最大序号(旧事件/状态回退)。 */
    STALE_SEQUENCE,
    /** 序号跳跃, 中间序号的事件尚未到达(乱序, 等待补齐, 不做覆盖)。 */
    OUT_OF_ORDER_GAP,
    /** 引用的 attempt 不存在(从未 STARTED), 或事件不属于该 attempt。 */
    UNKNOWN_ATTEMPT,
    /** 引用的 attempt 已经结束, 不能再次上报结局。 */
    ATTEMPT_ALREADY_FINISHED,
    /** occurredAt 早于该 run 已接收事件的 occurredAt(时钟回退)。 */
    TIMESTAMP_REGRESSION
}
