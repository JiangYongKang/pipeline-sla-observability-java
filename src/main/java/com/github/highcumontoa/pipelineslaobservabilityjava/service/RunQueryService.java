package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.Attempt;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunRecord;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunView;
import com.github.highcumontoa.pipelineslaobservabilityjava.store.EventStore;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 运行状态只读查询：在存储锁内取一致性快照，再映射为不可变视图。
 */
@Service
public class RunQueryService {

    private final EventStore store;

    public RunQueryService(EventStore store) {
        this.store = store;
    }

    public RunView get(String runId) {
        RunRecord rec;
        synchronized (store) {
            rec = store.getRecord(runId);
            if (rec == null) {
                return null;
            }
            // 在锁内完成拷贝，避免遍历期间被其他线程修改。
            return toView(rec);
        }
    }

    static RunView toView(RunRecord rec) {
        List<RunView.AttemptView> attempts = rec.getAttempts().stream()
                .map(RunQueryService::toAttemptView)
                .toList();
        return new RunView(
                rec.getRunId(),
                rec.getPipelineName(),
                rec.getState(),
                rec.getScheduledAt(),
                rec.getFirstStartedAt(),
                rec.getFinishedAt(),
                rec.getAttempts().size(),
                attempts);
    }

    private static RunView.AttemptView toAttemptView(Attempt a) {
        Long duration = (a.getStartedAt() != null && a.getFinishedAt() != null)
                ? a.getFinishedAt() - a.getStartedAt() : null;
        return new RunView.AttemptView(
                a.getAttempt(),
                a.getStartedAt(),
                a.getFinishedAt(),
                duration,
                a.isSucceeded(),
                a.isRetryableFailure(),
                a.getFailureReason(),
                a.getItemCount());
    }
}
