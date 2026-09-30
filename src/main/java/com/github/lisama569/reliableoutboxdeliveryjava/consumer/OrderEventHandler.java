package com.github.lisama569.reliableoutboxdeliveryjava.consumer;

import com.github.lisama569.reliableoutboxdeliveryjava.broker.MessageHandler;
import com.github.lisama569.reliableoutboxdeliveryjava.broker.QueueMessage;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcConsumerRepository;
import org.springframework.stereotype.Component;

@Component
public class OrderEventHandler implements MessageHandler {

    private final JdbcConsumerRepository consumerRepository;

    public OrderEventHandler(JdbcConsumerRepository consumerRepository) {
        this.consumerRepository = consumerRepository;
    }

    @Override
    public void handle(QueueMessage message) {
        // skeleton
    }
}
