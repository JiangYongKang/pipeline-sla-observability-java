package com.github.highcumontoa.pipelineslaobservabilityjava.controller;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.AggregationResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.PipelineEvent;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.SlaVerdict;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.IngestService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.MetricsService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.RunQueryService;
import com.github.highcumontoa.pipelineslaobservabilityjava.service.SlaService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** 事件接入 / 运行查询 / SLA / 聚合 HTTP 接口。 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final IngestService ingestService;
    private final RunQueryService runQueryService;
    private final SlaService slaService;
    private final MetricsService metricsService;

    public EventController(IngestService ingestService,
                           RunQueryService runQueryService,
                           SlaService slaService,
                           MetricsService metricsService) {
        this.ingestService = ingestService;
        this.runQueryService = runQueryService;
        this.slaService = slaService;
        this.metricsService = metricsService;
    }

    /** 提交事件；非法流转按原因返回 422，重复提交返回 200 但标记 duplicate。 */
    @PostMapping("/events")
    public ResponseEntity<IngestResult> ingest(@RequestBody PipelineEvent event) {
        IngestResult result = ingestService.ingest(event);
        if (result.accepted() || result.duplicate()) {
            return ResponseEntity.ok(result);
        }
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(result);
    }

    @GetMapping("/runs/{runId}")
    public RunView getRun(@PathVariable String runId) {
        return runQueryService.get(runId);
    }

    @GetMapping("/runs/{runId}/sla")
    public SlaVerdict getSla(@PathVariable String runId) {
        return slaService.evaluate(runId);
    }

    @GetMapping("/pipelines/{name}/aggregation")
    public AggregationResult aggregate(@PathVariable String name,
                                       @RequestParam long from,
                                       @RequestParam long to) {
        return metricsService.aggregate(name, from, to);
    }
}
