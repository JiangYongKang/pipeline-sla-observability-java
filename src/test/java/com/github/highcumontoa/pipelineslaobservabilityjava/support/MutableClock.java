package com.github.highcumontoa.pipelineslaobservabilityjava.support;

import com.github.highcumontoa.pipelineslaobservabilityjava.time.Clock;

import java.util.concurrent.atomic.AtomicLong;

/** 测试用时钟：可手动设置/推进，便于构造迟到事件与 SLA 边界。 */
public class MutableClock implements Clock {
    private final AtomicLong t;

    public MutableClock(long start) {
        this.t = new AtomicLong(start);
    }

    @Override
    public long nowMillis() {
        return t.get();
    }

    public void set(long millis) {
        t.set(millis);
    }

    public void advance(long deltaMillis) {
        t.addAndGet(deltaMillis);
    }
}
