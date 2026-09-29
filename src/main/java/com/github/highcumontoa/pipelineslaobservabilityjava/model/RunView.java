package com.github.highcumontoa.pipelineslaobservabilityjava.model;

import java.util.List;

/**
 * 运行状态的只读视图（查询接口返回）。
 * 不可变，保证调用方读到的是某一时刻的一致快照。
 */
public record RunView(
        String runId,
        String pipelineName,
        RunState state,
        long scheduledAt,
        Long firstStartedAt,
        Long finishedAt,
        int attemptCount,
        List<AttemptView> attempts
) {
    /** 单次尝试视图。 */
    public record AttemptView(
            int attempt,
            Long startedAt,
            Long finishedAt,
            Long durationMillis,
            boolean succeeded,
            boolean retryableFailure,
            String failureReason,
            long itemCount
    ) {
    }
}
