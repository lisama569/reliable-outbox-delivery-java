package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

import java.time.Instant;

/**
 * outbox 事件记录，与业务数据在同一个本地事务中落库。
 */
public class OutboxEvent {

    /** 事件唯一标识，同时作为消费端幂等键。 */
    private String id;
    /** 聚合类型，如 ORDER。 */
    private String aggregateType;
    /** 业务聚合标识（同一聚合的事件必须按序投递）。 */
    private String aggregateId;
    /** 事件类型，如 OrderCreated。 */
    private String eventType;
    /** 事件内容（JSON）。 */
    private String payload;

    private OutboxStatus status = OutboxStatus.PENDING;

    /** 当前这一轮投递已尝试次数（人工重投后归零）。 */
    private int attemptCount;
    /** 本轮最大尝试次数，超限进入死信。 */
    private int maxAttempts;
    /** 下次可被领取的时间（退避窗口）。 */
    private Instant nextRetryAt;

    /** 当前领取该事件的投递器标识。 */
    private String lockedBy;
    private Instant lockedAt;

    /** 进入死信时记录的失败原因。 */
    private String deadReason;
    /** 最近一次投递错误信息。 */
    private String lastError;
    /** 人工重投次数。 */
    private int redeliveryCount;

    private Instant createdAt;
    private Instant updatedAt;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public void setStatus(OutboxStatus status) {
        this.status = status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Instant getNextRetryAt() {
        return nextRetryAt;
    }

    public void setNextRetryAt(Instant nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    public Instant getLockedAt() {
        return lockedAt;
    }

    public void setLockedAt(Instant lockedAt) {
        this.lockedAt = lockedAt;
    }

    public String getDeadReason() {
        return deadReason;
    }

    public void setDeadReason(String deadReason) {
        this.deadReason = deadReason;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public int getRedeliveryCount() {
        return redeliveryCount;
    }

    public void setRedeliveryCount(int redeliveryCount) {
        this.redeliveryCount = redeliveryCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
