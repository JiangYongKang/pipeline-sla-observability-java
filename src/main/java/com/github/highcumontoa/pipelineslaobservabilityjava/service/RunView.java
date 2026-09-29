package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.SlaStatus;
import java.time.Instant;
import java.util.List;

/**
 * 单个运行的完整可解释视图: 状态、SLA 判定及其依据、尝试链路。
 */
public record RunView(
        String pipelineId,
        String runId,
        RunStatus status,
        Instant scheduledTime,
        Instant startedAt,
        Instant finishedAt,
        long slaMillis,
        Instant deadline,
        long freshnessDelayMillis,
        SlaStatus slaStatus,
        String slaReason,
        int attemptCount,
        int retriableFailureCount,
        Long durationMillis,
        Long processedCount,
        Double throughputPerSec,
        List<AttemptView> attempts
) {
}
