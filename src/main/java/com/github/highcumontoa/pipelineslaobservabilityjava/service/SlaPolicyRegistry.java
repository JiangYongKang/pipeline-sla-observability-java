package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SLA 策略注册表：按流水线名配置，缺省使用全局默认值。
 * 默认超时 60 分钟、最多尝试 3 次，可通过 application.properties 覆盖。
 */
@Component
public class SlaPolicyRegistry {

    private final SlaPolicy defaultPolicy;
    private final Map<String, SlaPolicy> policies = new ConcurrentHashMap<>();

    public SlaPolicyRegistry(
            @Value("${pipeline.sla.default-timeout-millis:3600000}") long defaultTimeoutMillis,
            @Value("${pipeline.sla.default-max-attempts:3}") int defaultMaxAttempts) {
        this.defaultPolicy = new SlaPolicy(defaultTimeoutMillis, defaultMaxAttempts);
    }

    /** 注册/更新某流水线的策略。 */
    public void register(String pipelineName, SlaPolicy policy) {
        policies.put(pipelineName, policy);
    }

    public SlaPolicy policyFor(String pipelineName) {
        return Optional.ofNullable(policies.get(pipelineName)).orElse(defaultPolicy);
    }
}
