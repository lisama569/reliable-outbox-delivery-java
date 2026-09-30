package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

import com.github.lisama569.reliableoutboxdeliveryjava.broker.MessagePublisher;
import com.github.lisama569.reliableoutboxdeliveryjava.config.OutboxProperties;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 启动多个并发的投递器实例。每个实例有唯一 workerId，
 * 它们并发轮询同一张 outbox 表，靠数据库原子领取互斥（同一事件不会被领取两次）。
 */
@Component
public class DispatcherManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DispatcherManager.class);

    private final JdbcOutboxRepository repository;
    private final MessagePublisher publisher;
    private final BackoffPolicy backoffPolicy;
    private final OutboxProperties properties;
    private final DispatchInterceptors interceptors;
    private final List<OutboxDispatcher> dispatchers = new ArrayList<>();
    private volatile boolean running;

    public DispatcherManager(JdbcOutboxRepository repository,
                             MessagePublisher publisher,
                             BackoffPolicy backoffPolicy,
                             OutboxProperties properties,
                             List<DispatchInterceptors> interceptors) {
        this.repository = repository;
        this.publisher = publisher;
        this.backoffPolicy = backoffPolicy;
        this.properties = properties;
        this.interceptors = interceptors.isEmpty()
                ? new DispatchInterceptors() {
                }
                : new CompositeDispatchInterceptors(interceptors);
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        if (!properties.isEnabled()) {
            log.info("[dispatch] outbox dispatch disabled by configuration");
            return;
        }
        int count = Math.max(1, properties.getDispatcherInstances());
        for (int i = 1; i <= count; i++) {
            String workerId = "worker-" + i;
            OutboxDispatcher dispatcher = new OutboxDispatcher(workerId, repository, publisher,
                    backoffPolicy, interceptors, properties);
            dispatcher.start();
            dispatchers.add(dispatcher);
        }
        running = true;
        log.info("[dispatch] {} dispatcher instance(s) running", dispatchers.size());
    }

    @Override
    public void stop() {
        dispatchers.forEach(OutboxDispatcher::stop);
        dispatchers.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 200;
    }

    private static final class CompositeDispatchInterceptors implements DispatchInterceptors {

        private final List<DispatchInterceptors> delegates;

        private CompositeDispatchInterceptors(List<DispatchInterceptors> delegates) {
            this.delegates = delegates;
        }

        @Override
        public void beforeMarkDelivered(
                com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent event) {
            delegates.forEach(d -> d.beforeMarkDelivered(event));
        }
    }
}
