package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/** 一次尝试（同一 runId 下每次 STARTED..FAILED/SUCCEEDED 为一个 attempt）。 */
public class Attempt {
    private int attempt;
    private Long startedAt;
    private Long finishedAt;
    private boolean succeeded;
    private boolean retryableFailure;
    private String failureReason;
    private long itemCount;

    public Attempt() {
    }

    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public Long getStartedAt() { return startedAt; }
    public void setStartedAt(Long startedAt) { this.startedAt = startedAt; }
    public Long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Long finishedAt) { this.finishedAt = finishedAt; }
    public boolean isSucceeded() { return succeeded; }
    public void setSucceeded(boolean succeeded) { this.succeeded = succeeded; }
    public boolean isRetryableFailure() { return retryableFailure; }
    public void setRetryableFailure(boolean retryableFailure) { this.retryableFailure = retryableFailure; }
    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    public long getItemCount() { return itemCount; }
    public void setItemCount(long itemCount) { this.itemCount = itemCount; }
}
