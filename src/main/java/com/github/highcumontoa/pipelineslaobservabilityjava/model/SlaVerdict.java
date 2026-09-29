package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/**
 * 单次运行的 SLA 判定结论及其依据（可解释）。
 */
public record SlaVerdict(
        String runId,
        String pipelineName,
        SlaStatus status,
        long scheduledAt,
        long deadline,
        Long successAt,
        Long delayMillis,
        boolean timedOut,
        boolean recovered,
        String basis
) {
}
