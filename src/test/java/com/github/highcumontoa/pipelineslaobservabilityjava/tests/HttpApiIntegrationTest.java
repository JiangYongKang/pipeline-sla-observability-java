package com.github.highcumontoa.pipelineslaobservabilityjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端集成测试：真实 Spring 容器 + HTTP，
 * 验证接受(200)/非法流转(422)/重复(200+duplicate) 状态码，
 * 以及运行查询、SLA、聚合接口可解释输出。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpApiIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    ObjectMapper mapper;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private Map<String, Object> event(String eventId, String runId, String type,
                                      int attempt, long time, Boolean retryable, String reason) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", eventId);
        m.put("runId", runId);
        m.put("pipelineName", "http-pipe");
        m.put("type", type);
        m.put("attempt", attempt);
        m.put("eventTime", time);
        m.put("retryable", retryable);
        m.put("failureReason", reason);
        m.put("itemCount", 10L);
        return m;
    }

    @Test
    void fullLifecycleOverHttp() throws Exception {
        String run = "http-run-1";
        postAndAssert(event("h-s", run, "SCHEDULED", 0, 0, null, null),
                HttpStatus.OK, true, false, null);
        postAndAssert(event("h-t1", run, "STARTED", 1, 10, null, null),
                HttpStatus.OK, true, false, null);

        // 乱序：第 2 次尝试完成先于第 1 次 -> 422 ATTEMPT_MISMATCH
        ResponseEntity<String> bad = post(event("h-x", run, "SUCCEEDED", 2, 20, null, null));
        System.out.println("[HTTP 乱序拒绝] " + bad.getStatusCode() + " " + bad.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, bad.getStatusCode());
        assertTrue(bad.getBody().contains("ATTEMPT_MISMATCH"));

        // 正常第 1 次成功
        postAndAssert(event("h-o1", run, "SUCCEEDED", 1, 500, null, null),
                HttpStatus.OK, true, false, null);

        // 终态后再来事件 -> 422 TERMINAL_RUN
        ResponseEntity<String> terminal = post(event("h-t2", run, "STARTED", 2, 600, null, null));
        System.out.println("[HTTP 终态拒绝] " + terminal.getStatusCode() + " " + terminal.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, terminal.getStatusCode());
        assertTrue(terminal.getBody().contains("TERMINAL_RUN"));

        // 重复提交：200 且 duplicate=true
        ResponseEntity<String> dup = post(event("h-s", run, "SCHEDULED", 0, 0, null, null));
        System.out.println("[HTTP 重复提交] " + dup.getStatusCode() + " " + dup.getBody());
        assertEquals(HttpStatus.OK, dup.getStatusCode());
        JsonNode dupNode = mapper.readTree(dup.getBody());
        assertFalse(dupNode.get("accepted").asBoolean());
        assertTrue(dupNode.get("duplicate").asBoolean());
        assertEquals("DUPLICATE_EVENT", dupNode.get("reason").asText());

        // 查询接口
        String runJson = rest.getForObject(url("/api/runs/" + run), String.class);
        System.out.println("[HTTP 运行查询] " + runJson);
        assertTrue(runJson.contains("SUCCEEDED"));

        String slaJson = rest.getForObject(url("/api/runs/" + run + "/sla"), String.class);
        System.out.println("[HTTP SLA] " + slaJson);
        JsonNode sla = mapper.readTree(slaJson);
        assertEquals("ON_TIME", sla.get("status").asText());
        assertTrue(sla.get("basis").asText().length() > 0, "SLA 必须给出判定依据");

        String aggJson = rest.getForObject(
                url("/api/pipelines/http-pipe/aggregation?from=0&to=9999999999999"), String.class);
        System.out.println("[HTTP 聚合] " + aggJson);
        JsonNode agg = mapper.readTree(aggJson);
        assertTrue(agg.get("totalRuns").asInt() >= 1);
    }

    private ResponseEntity<String> post(Map<String, Object> body) {
        return rest.postForEntity(url("/api/events"), new HttpEntity<>(body), String.class);
    }

    private void postAndAssert(Map<String, Object> body, HttpStatus status,
                               boolean accepted, boolean duplicate, String reason) {
        ResponseEntity<String> resp = post(body);
        System.out.println("[HTTP 输入] " + body.get("eventId") + "/" + body.get("type")
                + " => " + resp.getStatusCode() + " " + resp.getBody());
        assertEquals(status, resp.getStatusCode());
        try {
            JsonNode node = mapper.readTree(resp.getBody());
            assertEquals(accepted, node.get("accepted").asBoolean());
            assertEquals(duplicate, node.get("duplicate").asBoolean());
            if (reason != null) {
                assertEquals(reason, node.get("reason").asText());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
