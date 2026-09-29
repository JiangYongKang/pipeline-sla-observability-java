package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.AttemptOutcome;
import java.time.Instant;

/** 单次尝试视图。durationMillis 为结束-开始; 进行中的尝试为 null。 */
public record AttemptView(
        long attemptSeq,
        Instant startedAt,
        Instant finishedAt,
        Long durationMillis,
        AttemptOutcome outcome,
        String errorCode,
        long processedCount
) {
}
