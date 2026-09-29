package com.github.highcumontoa.pipelineslaobservabilityjava.model;

/** SLA 新鲜度判定结论。 */
public enum SlaStatus {
    /** 尚未到截止时间，运行中或等待重试。 */
    ON_TRACK,
    /** 在截止时间内成功。 */
    ON_TIME,
    /** 已过截止时间但运行仍未成功（超时，终态失败或仍在跑）。 */
    TIMEOUT,
    /** 截止时间之后才成功：属于违约的迟到成功，不再波动为 ON_TIME。 */
    LATE_SUCCESS
}
