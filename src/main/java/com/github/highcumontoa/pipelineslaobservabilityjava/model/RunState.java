package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/** 运行的聚合状态（由已接受事件归约得到）。 */
public enum RunState {
    /** 仅收到 SCHEDULED，尚未开始。 */
    SCHEDULED,
    /** 最近一次尝试正在运行。 */
    RUNNING,
    /** 最近一次尝试成功，运行终态。 */
    SUCCEEDED,
    /** 最近一次尝试失败且仍可重试（等待下一次 STARTED）。 */
    AWAITING_RETRY,
    /** 终态失败（不可重试，或重试次数耗尽）。 */
    FAILED_FINAL
}
