package com.github.lisama569.reliableoutboxdeliveryjava.web;

import java.time.Instant;

/**
 * Web 层请求/响应 DTO 集合。
 */
public final class Dtos {

    private Dtos() {
    }

    public record CreateOrderRequest(String customer) {
    }

    public record CreateOrderResponse(String orderId, String eventId) {
    }

    public record ShipOrderResponse(String orderId, String eventId) {
    }

    public record EventView(
            String id,
            String aggregateType,
            String aggregateId,
            String eventType,
            String status,
            int attemptCount,
            int maxAttempts,
            int redeliveryCount,
            Instant nextRetryAt,
            String deadReason,
            String lastError,
            String lockedBy,
            Instant createdAt
    ) {
    }

    public record RedriveResponse(String eventId, String status, int redeliveryCount) {
    }
}
