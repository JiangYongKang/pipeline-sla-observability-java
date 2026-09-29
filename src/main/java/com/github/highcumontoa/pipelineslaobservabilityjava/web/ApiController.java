package com.github.highcumontoa.pipelineslaobservabilityjava.web;

import com.github.highcumontoa.pipelineslaobservabilityjava.domain.EventRejectedException;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.domain.RejectReason;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.EventIngestionService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST 接入与查询接口。
 * <ul>
 *   <li>POST /api/events、/api/events/batch: 事件接入;</li>
 *   <li>GET  /api/runs、/api/runs/{runId}: 运行状态查询;</li>
 *   <li>GET  /api/pipelines/{pipelineId}/metrics: 流水线聚合指标;</li>
 *   <li>POST /api/sla/sweep: 以当前时钟推进 SLA 扫描(模拟调度周期到来)。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class ApiController {

    private final EventIngestionService ingestionService;
    private final MetricsService metricsService;
    private final SlaService slaService;

    public ApiController(EventIngestionService ingestionService,
                         MetricsService metricsService,
                         SlaService slaService) {
        this.ingestionService = ingestionService;
        this.metricsService = metricsService;
        this.slaService = slaService;
    }

    @PostMapping("/events")
    public IngestResult ingest(@RequestBody PipelineEvent event) {
        return ingestionService.ingest(event);
    }

    @PostMapping("/events/batch")
    public List<IngestResult> ingestBatch(@RequestBody List<PipelineEvent> events) {
        return ingestionService.ingestBatch(events);
    }

    @GetMapping("/runs/{runId}")
    public ResponseEntity<RunView> run(@PathVariable String runId) {
        RunView view = metricsService.getRun(runId);
        return view == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(view);
    }

    @GetMapping("/runs")
    public List<RunView> runs() {
        return metricsService.allRuns();
    }

    @GetMapping("/pipelines/{pipelineId}/metrics")
    public MetricsView metrics(@PathVariable String pipelineId) {
        return metricsService.pipelineMetrics(pipelineId);
    }

    @PostMapping("/sla/sweep")
    public Map<String, Integer> sweep() {
        int open = slaService.sweepNow();
        return Map.of("openTimeoutRuns", open);
    }

    /** 非法事件: 409 + 结构化原因, 调用方可按原因区分而非得到静默成功。 */
    @ExceptionHandler(EventRejectedException.class)
    public ResponseEntity<Map<String, Object>> handleRejected(EventRejectedException ex) {
        Map<String, Object> body = Map.of(
                "accepted", false,
                "reason", ex.getReason().name(),
                "eventId", nullToEmpty(ex.getEventId()),
                "runId", nullToEmpty(ex.getRunId()),
                "message", ex.getMessage()
        );
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    /** 兜底: 载荷缺失等。 */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegal(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(Map.of("accepted", false, "reason", RejectReason.INVALID_PAYLOAD.name(),
                        "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }
}
