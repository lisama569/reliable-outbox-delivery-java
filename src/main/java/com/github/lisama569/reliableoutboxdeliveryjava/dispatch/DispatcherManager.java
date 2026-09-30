package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

import com.github.lisama569.reliableoutboxdeliveryjava.broker.MessagePublisher;
import com.github.lisama569.reliableoutboxdeliveryjava.config.OutboxProperties;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DispatcherManager {

    private final JdbcOutboxRepository repository;
    private final MessagePublisher publisher;
    private final BackoffPolicy backoffPolicy;
    private final OutboxProperties properties;
    private final List<DispatchInterceptors> interceptors;
    private final List<OutboxDispatcher> dispatchers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    public DispatcherManager(JdbcOutboxRepository repository,
                             MessagePublisher publisher,
                             BackoffPolicy backoffPolicy,
                             OutboxProperties properties,
                             List<DispatchInterceptors> interceptors) {
        this.repository = repository;
        this.publisher = publisher;
        this.backoffPolicy = backoffPolicy;
        this.properties = properties;
        this.interceptors = interceptors;
    }

    public void start() {
        // skeleton
    }

    public void stop() {
        // skeleton
    }
}
