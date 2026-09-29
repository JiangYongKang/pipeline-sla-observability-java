package com.github.highcumontoa.pipelineslaobservabilityjava.time;

/** 可替换的时钟，便于在测试中构造乱序、迟到与 SLA 边界场景。 */
public interface Clock {
    long nowMillis();
}
