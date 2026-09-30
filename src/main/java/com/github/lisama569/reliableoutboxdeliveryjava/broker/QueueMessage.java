package com.github.lisama569.reliableoutboxdeliveryjava.broker;

import java.time.Instant;

public class QueueMessage {

    private final String eventId;
    private final String aggregateType;
    private final String aggregateId;
    private final String eventType;
    private final String payload;
    private final Instant enqueuedAt;

    public QueueMessage(String eventId, String aggregateType, String aggregateId,
                        String eventType, String payload, Instant enqueuedAt) {
        this.eventId = eventId;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.enqueuedAt = enqueuedAt;
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

    public Instant getEnqueuedAt() {
        return enqueuedAt;
    }
}
