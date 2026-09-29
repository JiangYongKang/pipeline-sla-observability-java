package com.github.highcumontoa.pipelineslaobservabilityjava.service;

import com.github.highcumontoa.pipelineslaobservabilityjava.model.IngestResult;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RejectReason;
import com.github.highcumontoa.pipelineslaobservabilityjava.model.RunState;

/** 接入结果构造辅助。 */
final class IngestResults {
    private IngestResults() {
    }

    static IngestResult reject(RejectReason reason, String message, RunState state) {
        return new IngestResult(false, false, reason, state, message);
    }
}
