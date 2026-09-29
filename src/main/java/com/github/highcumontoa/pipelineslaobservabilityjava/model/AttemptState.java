package com.github.highcumontoa.pipelineslaobservabilityjava.model;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.AttemptOutcome;
import java.time.Instant;

/**
 * 单次尝试的可变状态。字段公开以便 Jackson 做本地快照序列化。
 */
public class AttemptState {
    public long attemptSeq;
    public Instant startedAt;
    public Instant finishedAt;
    public AttemptOutcome outcome;
    public String errorCode;
    public long processedCount;

    public AttemptState() {
    }
}
