package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/**
 * 一组运行（某流水线/时间窗）的聚合指标。
 */
public record AggregationResult(
        String pipelineName,
        long windowStart,
        long windowEnd,
        int totalRuns,
        int succeededRuns,
        int finalFailedRuns,
        int timeoutRuns,
        int lateSuccessRuns,
        int retryableFailures,
        long totalAttempts,
        double averageAttemptsPerRun,
        Long totalRunDurationMillis,
        Long averageRunDurationMillis,
        double totalThroughputItemsPerSec,
        Double averageThroughputItemsPerSec,
        String basis
) {
}
