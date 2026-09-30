package com.github.lisama569.reliableoutboxdeliveryjava.broker;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * 在投递器启动之前装配消费者并启动本地消息队列。
 */
@Component
public class BrokerLifecycle implements SmartLifecycle {

    private final LocalMessageBroker broker;
    private final MessageHandler handler;
    private volatile boolean running;

    public BrokerLifecycle(LocalMessageBroker broker, MessageHandler handler) {
        this.broker = broker;
        this.handler = handler;
    }

    @Override
    public void start() {
        broker.subscribe(handler);
        broker.start();
        running = true;
    }

    @Override
    public void stop() {
        broker.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 100;
    }
}
