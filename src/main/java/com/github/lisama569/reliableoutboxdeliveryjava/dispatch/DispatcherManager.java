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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 启动多个并发的投递器实例。每个实例有唯一 workerId，
 * 它们并发轮询同一张 outbox 表，靠数据库原子领取互斥（同一事件不会被领取两次）。
 *
 * 某个 worker 线程异常终止（模拟所在进程被杀）时，管理器会在可见性超时后
 * 用同一个 workerId 重新拉起一个 worker；它会通过孤儿回收接管未完成事件。
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
    private ScheduledExecutorService supervisor;
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
    public synchronized void start() {
        if (running) {
            return;
        }
        if (!properties.isEnabled()) {
            log.info("[dispatch] outbox dispatch disabled by configuration");
            return;
        }
        supervisor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-dispatcher-supervisor");
            t.setDaemon(true);
            return t;
        });
        int count = Math.max(1, properties.getDispatcherInstances());
        for (int i = 1; i <= count; i++) {
            launch("worker-" + i);
        }
        running = true;
        log.info("[dispatch] {} dispatcher instance(s) running", dispatchers.size());
    }

    private synchronized void launch(String workerId) {
        OutboxDispatcher dispatcher = new OutboxDispatcher(workerId, repository, publisher,
                backoffPolicy, interceptors, properties);
        dispatcher.start(() -> scheduleRestart(workerId));
        dispatchers.add(dispatcher);
    }

    private void scheduleRestart(String workerId) {
        long delayMs = properties.getVisibilityTimeout().toMillis()
                + properties.getPollInterval().toMillis();
        log.warn("[dispatch] scheduling failover restart of {} in {} ms", workerId, delayMs);
        supervisor.schedule(() -> {
            synchronized (this) {
                if (!running) {
                    return;
                }
                log.warn("[dispatch] failover: relaunching {}", workerId);
                launch(workerId);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (supervisor != null) {
            supervisor.shutdownNow();
        }
        dispatchers.forEach(OutboxDispatcher::stop);
        dispatchers.clear();
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
