package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

/**
 * 对事件执行了当前状态不允许的流转。
 */
public class InvalidEventStateException extends RuntimeException {

    private final String eventId;
    private final OutboxStatus currentStatus;

    public InvalidEventStateException(String eventId, OutboxStatus currentStatus, String operation) {
        super("operation '" + operation + "' is not allowed in status " + currentStatus
                + " for event " + eventId);
        this.eventId = eventId;
        this.currentStatus = currentStatus;
    }

    public String getEventId() {
        return eventId;
    }

    public OutboxStatus getCurrentStatus() {
        return currentStatus;
    }
}
