package com.github.highcumontoa.pipelineslaobservabilityjava.time;

import org.springframework.stereotype.Component;

/** 基于系统墙钟的默认实现。 */
@Component
public class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
