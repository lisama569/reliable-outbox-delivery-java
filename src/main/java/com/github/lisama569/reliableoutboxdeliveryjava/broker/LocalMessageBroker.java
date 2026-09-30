package com.github.lisama569.reliableoutboxdeliveryjava.broker;

import org.springframework.stereotype.Component;

@Component
public class LocalMessageBroker implements MessagePublisher {

    @Override
    public void publish(QueueMessage message)
            throws RetryableDeliveryException, NonRetryableDeliveryException {
        // skeleton
    }

    public void subscribe(MessageHandler handler) {
        // skeleton
    }

    public void reset() {
        // skeleton
    }
}
