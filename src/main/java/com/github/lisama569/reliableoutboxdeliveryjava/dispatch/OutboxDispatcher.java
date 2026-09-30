package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

import com.github.lisama569.reliableoutboxdeliveryjava.broker.MessagePublisher;
import com.github.lisama569.reliableoutboxdeliveryjava.config.OutboxProperties;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboxDispatcher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final String workerId;
    private final JdbcOutboxRepository repository;
    private final MessagePublisher publisher;
    private final BackoffPolicy backoffPolicy;
    private final DispatchInterceptors interceptors;
    private final OutboxProperties properties;

    private volatile boolean running;

    public OutboxDispatcher(String workerId,
                            JdbcOutboxRepository repository,
                            MessagePublisher publisher,
                            BackoffPolicy backoffPolicy,
                            DispatchInterceptors interceptors,
                            OutboxProperties properties) {
        this.workerId = workerId;
        this.repository = repository;
        this.publisher = publisher;
        this.backoffPolicy = backoffPolicy;
        this.interceptors = interceptors;
        this.properties = properties;
    }

    @Override
    public void run() {
        while (running) {
            // skeleton
        }
    }

    public String getWorkerId() {
        return workerId;
    }
}
