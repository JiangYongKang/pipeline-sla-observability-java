package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.EventIngestionService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.RunStore;
import com.github.highcumontoa.pipelineslaobservabilityjava.web.ApiController;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** Web 层: 接入 200、拒绝 409 且携带结构化 reason、查询与聚合。 */
class WebApiTest {

    private static final Instant T0 = Instant.parse("2026-09-29T00:00:00Z");

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(T0.plusSeconds(3600), ZoneOffset.UTC);
        RunStore store = new RunStore(new ObjectMapper(), "");
        SlaService sla = new SlaService(clock, store);
        EventIngestionService ingestion = new EventIngestionService(store, sla);
        MetricsService metrics = new MetricsService(store, sla, clock);
        mvc = MockMvcBuilders.standaloneSetup(new ApiController(ingestion, metrics, sla)).build();
    }

    @Test
    void acceptEventThenQueryRun() throws Exception {
        PipelineEvent start = Events.runStarted("w1", "p", "wrun", 1, T0, T0, 60_000);
        mvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(start)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.duplicate").value(false));

        mvc.perform(get("/api/runs/wrun"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.runId").value("wrun"))
                .andExpect(jsonPath("$.status").value("RUNNING"))
                // 查询时刻(固定为 T0+3600)已超过 60s deadline => OPEN
                .andExpect(jsonPath("$.slaStatus").value("OPEN"));
    }

    @Test
    void rejectedEventReturns409WithReason() throws Exception {
        PipelineEvent start = Events.runStarted("w1", "p", "wrun2", 1, T0, T0, 60_000);
        PipelineEvent duplicate = Events.runStarted("w1", "p", "wrun2", 1, T0, T0, 60_000);
        // 第一次接受
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(start))).andExpect(status().isOk());
        // 完全重复 -> 仍 200, duplicate=true(幂等, 不是错误)
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(duplicate)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true));

        // 第二个不同的 RUN_STARTED -> 409 DUPLICATE_RUN
        PipelineEvent secondStart = Events.runStarted("w9", "p", "wrun2", 2,
                T0.plusMillis(5), T0, 60_000);
        System.out.println("[HTTP] 重复 RUN_STARTED 期望 409 + reason=DUPLICATE_RUN");
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(secondStart)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.reason").value("DUPLICATE_RUN"));
    }

    @Test
    void metricsAndSweepEndpointsWork() throws Exception {
        PipelineEvent start = Events.runStarted("m1", "pmp", "mrun", 1, T0, T0, 60_000);
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(start))).andExpect(status().isOk());

        mvc.perform(get("/api/pipelines/pmp/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.pipelineId").value("pmp"))
                .andExpect(jsonPath("$.totalRuns").value(1))
                .andExpect(jsonPath("$.runningRuns").value(1));

        // 固定时钟为 T0+3600, sla 60s, 扫描应开启超时
        mvc.perform(post("/api/sla/sweep"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.openTimeoutRuns").value(1));

        mvc.perform(get("/api/runs/mrun"))
                .andExpect(jsonPath("$.slaStatus").value("OPEN"));
    }
}
