package com.github.highcumontoa.pipelineslaobservabilityjava.domain;

/** 单次尝试(attempt)的结局。 */
public enum AttemptOutcome {
    RETRIABLE_FAILURE,
    SUCCEEDED
}
