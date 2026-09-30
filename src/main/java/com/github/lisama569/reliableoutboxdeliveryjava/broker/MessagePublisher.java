package com.github.lisama569.reliableoutboxdeliveryjava.broker;

public interface MessagePublisher {

    void publish(QueueMessage message)
            throws RetryableDeliveryException, NonRetryableDeliveryException;
}
