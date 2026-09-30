package com.github.lisama569.reliableoutboxdeliveryjava.web;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;

import java.time.Instant;

public class EventView {

    private long id;
    private String eventId;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private String payload;
    private String status;
    private int attempts;
    private Instant nextAttemptAt;
    private String lockedBy;
    private Instant lockedAt;
    private String lastError;
    private String deadReason;
    private Instant deadAt;
    private Instant createdAt;
    private Instant updatedAt;

    public static EventView from(OutboxEvent e) {
        EventView v = new EventView();
        v.id = e.getId();
        v.eventId = e.getEventId();
        v.aggregateType = e.getAggregateType();
        v.aggregateId = e.getAggregateId();
        v.eventType = e.getEventType().name();
        v.payload = e.getPayload();
        v.status = e.getStatus().name();
        v.attempts = e.getAttempts();
        v.nextAttemptAt = e.getNextAttemptAt();
        v.lockedBy = e.getLockedBy();
        v.lockedAt = e.getLockedAt();
        v.lastError = e.getLastError();
        v.deadReason = e.getDeadReason();
        v.deadAt = e.getDeadAt();
        v.createdAt = e.getCreatedAt();
        v.updatedAt = e.getUpdatedAt();
        return v;
    }

    public long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public String getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public Instant getLockedAt() {
        return lockedAt;
    }

    public String getLastError() {
        return lastError;
    }

    public String getDeadReason() {
        return deadReason;
    }

    public Instant getDeadAt() {
        return deadAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
