package com.github.highcumontoa.pipelineslaobservabilityjava.domain;

/**
 * 流水线运行事件类型。
 *
 * <p>一次运行(run)的正常生命周期:
 * <pre>
 * RUN_STARTED
 *   -> ATTEMPT_STARTED -> (ATTEMPT_RETRIABLE_FAILURE | ATTEMPT_SUCCEEDED)   (可重复多轮)
 *   -> (RUN_SUCCEEDED | RUN_FAILED)
 * </pre>
 */
public enum EventType {
    RUN_STARTED,
    ATTEMPT_STARTED,
    ATTEMPT_RETRIABLE_FAILURE,
    ATTEMPT_SUCCEEDED,
    RUN_SUCCEEDED,
    RUN_FAILED
}
