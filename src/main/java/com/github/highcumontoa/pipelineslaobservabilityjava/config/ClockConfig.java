package com.github.highcumontoa.pipelineslaobservabilityjava.config;

import java.time.Clock;
import java.time.ZoneOffset;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 提供统一的 UTC 时钟, 测试中可替换为可控时钟。 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock systemClock() {
        return Clock.systemUTC();
    }
}
