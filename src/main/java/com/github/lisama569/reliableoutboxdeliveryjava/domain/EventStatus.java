package com.github.lisama569.reliableoutboxdeliveryjava.domain;

public enum EventStatus {
    PENDING,
    PROCESSING,
    RETRY,
    DEAD,
    DELIVERED,
    RESOLVED
}
