package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/**
 * SLA 策略。
 *
 * @param timeoutMillis 允许的最大运行时长（从调度时间起算）
 * @param maxAttempts   最大尝试次数（1 表示不允许重试）
 */
public record SlaPolicy(long timeoutMillis, int maxAttempts) {
}
