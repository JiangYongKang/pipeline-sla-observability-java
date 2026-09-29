package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import java.util.List;

/**
 * 单个流水线的聚合指标。无论事件到达顺序如何、或服务是否从快照恢复,
 * 结果都只由已接受事件的最终状态决定, 因此稳定一致。
 */
public record MetricsView(
        String pipelineId,
        int totalRuns,
        int runningRuns,
        int succeededRuns,
        int failedRuns,
        int retriableFailureCount,
        int totalAttempts,
        int slaPending,
        int slaOpen,
        int slaMet,
        int slaBreached,
        int timeoutIncidentsOpened,
        int timeoutIncidentsBreached,
        int timeoutIncidentsRecovered,
        long totalProcessedCount,
        Long totalRunDurationMillis,
        Double aggregateThroughputPerSec,
        List<RunView> runs
) {
}
