package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

/**
 * 指定事件不存在。
 */
public class EventNotFoundException extends RuntimeException {

    private final String eventId;

    public EventNotFoundException(String eventId) {
        super("event not found: " + eventId);
        this.eventId = eventId;
    }

    public String getEventId() {
        return eventId;
    }
}
