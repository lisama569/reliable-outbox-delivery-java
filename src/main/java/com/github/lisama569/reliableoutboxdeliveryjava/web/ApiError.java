package com.github.lisama569.reliableoutboxdeliveryjava.web;

import java.time.Instant;

/**
 * 统一错误响应体。
 */
public record ApiError(
        String code,
        String message,
        String eventId,
        Instant timestamp
) {
    public static ApiError of(ErrorCode code, String message, String eventId) {
        return new ApiError(code.name(), message, eventId, Instant.now());
    }
}
