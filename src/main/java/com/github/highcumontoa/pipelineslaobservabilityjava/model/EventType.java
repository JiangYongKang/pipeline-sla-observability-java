package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/**
 * 流水线运行生命周期事件类型。
 * 合法主链：SCHEDULED -> STARTED -> (SUCCEEDED | FAILED)
 * 可重试失败后允许以 attempt+1 重新 STARTED，形成重试链。
 */
public enum EventType {
    SCHEDULED,
    STARTED,
    SUCCEEDED,
    FAILED
}
