package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/**
 * 事件被拒绝的原因分类——重复、乱序、回退等不做静默处理，
 * 一律按原因显式拒绝。
 */
public enum RejectReason {
    /** 事件 ID 重复提交（幂等去重，不作为新事件接受）。 */
    DUPLICATE_EVENT,
    /** runId 未知：收到生命周期事件但从未 SCHEDULED。 */
    UNKNOWN_RUN,
    /** 第一个事件必须是 SCHEDULED。 */
    FIRST_EVENT_MUST_BE_SCHEDULED,
    /** 状态回退：例如 STARTED 之后再来 SCHEDULED，或 SUCCEEDED 后收到 STARTED。 */
    ILLEGAL_TRANSITION,
    /** 运行已处于终态（SUCCEEDED / FAILED_FINAL），拒绝任何后续事件。 */
    TERMINAL_RUN,
    /** 事件携带的 attempt 序号与重试链不一致（乱序/串线）。 */
    ATTEMPT_MISMATCH,
    /** 事件时间戳相对当前运行状态回退（eventTime 早于已接受时间）。 */
    STALE_TIMESTAMP,
    /** 同一 attempt 的 STARTED 重复（该尝试已经开始）。 */
    ATTEMPT_ALREADY_STARTED,
    /** FAILED 事件缺少原因字段。 */
    MISSING_FAILURE_REASON
}
