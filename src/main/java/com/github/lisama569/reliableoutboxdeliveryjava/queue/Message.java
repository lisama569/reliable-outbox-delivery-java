package com.github.lisama569.reliableoutboxdeliveryjava.queue;

/**
 * 队列中流转的一条消息。deliveryId 由队列分配（每次重投都不同），
 * eventId 是业务事件标识，作为消费端幂等键。
 */
public class Message {

    private final String deliveryId;
    private final String queue;
    private final String eventId;
    private final byte[] payload;
    private int deliveryCount;

    public Message(String deliveryId, String queue, String eventId, byte[] payload, int deliveryCount) {
        this.deliveryId = deliveryId;
        this.queue = queue;
        this.eventId = eventId;
        this.payload = payload;
        this.deliveryCount = deliveryCount;
    }

    public String getDeliveryId() {
        return deliveryId;
    }

    public String getQueue() {
        return queue;
    }

    public String getEventId() {
        return eventId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int getDeliveryCount() {
        return deliveryCount;
    }

    public void incrementDeliveryCount() {
        this.deliveryCount++;
    }

    public Message nextRedelivery(String newDeliveryId) {
        return new Message(newDeliveryId, queue, eventId, payload, deliveryCount + 1);
    }
}
