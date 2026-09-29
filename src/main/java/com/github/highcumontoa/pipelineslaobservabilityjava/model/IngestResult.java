package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/**
 * 单次事件接入结果。
 *
 * @param accepted 是否接受（false 时 reason 给出拒绝原因）
 * @param duplicate 是否为重复事件（被幂等识别）
 * @param reason    拒绝原因，接受时为 null
 * @param runState  处理后运行所处状态
 * @param message   人类可解释说明
 */
public record IngestResult(
        boolean accepted,
        boolean duplicate,
        RejectReason reason,
        RunState runState,
        String message
) {
}
