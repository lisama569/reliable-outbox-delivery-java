package com.github.lisama569.reliableoutboxdeliveryjava.broker;

public interface MessageHandler {

    void handle(QueueMessage message) throws RetryableDeliveryException, NonRetryableDeliveryException;
}
