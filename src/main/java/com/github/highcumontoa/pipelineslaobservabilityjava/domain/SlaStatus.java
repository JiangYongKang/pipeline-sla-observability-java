package com.github.highcumontoa.pipelineslaobservabilityjava.domain;

/**
 * 运行的 SLA 判定结论:
 * <ul>
 *   <li>{@link #PENDING} - 运行进行中且尚未被观察到超时;</li>
 *   <li>{@link #OPEN} - 运行进行中, 监控时刻已超过 deadline, 超时事件已开启(只开一次);</li>
 *   <li>{@link #MET} - 运行在 deadline 之前结束(成功或失败);</li>
 *   <li>{@link #BREACHED} - 运行在 deadline 之后才结束, 或结束前已被观察到超时。</li>
 * </ul>
 */
public enum SlaStatus {
    PENDING,
    OPEN,
    MET,
    BREACHED
}
