package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

import com.github.lisama569.reliableoutboxdeliveryjava.broker.NonRetryableDeliveryException;
import com.github.lisama569.reliableoutboxdeliveryjava.broker.QueueMessage;
import com.github.lisama569.reliableoutboxdeliveryjava.broker.RetryableDeliveryException;
import com.github.lisama569.reliableoutboxdeliveryjava.broker.MessagePublisher;
import com.github.lisama569.reliableoutboxdeliveryjava.config.OutboxProperties;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * 后台投递器：轮询领取到期事件 -> 发布到本地消息队列并等待消费确认 ->
 * 根据结果标记 DELIVERED / 安排退避重试 / 转入死信。
 * 每个实例是一个单线程 worker；多个实例并发运行时靠数据库原子领取保证不重复处理。
 */
public class OutboxDispatcher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final String workerId;
    private final JdbcOutboxRepository repository;
    private final MessagePublisher publisher;
    private final BackoffPolicy backoffPolicy;
    private final DispatchInterceptors interceptors;
    private final OutboxProperties properties;

    private volatile boolean running;
    private long lastReclaimMillis;

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

    public void start() {
        running = true;
        lastReclaimMillis = System.currentTimeMillis();
        Thread thread = new Thread(this, "outbox-dispatcher-" + workerId);
        thread.setDaemon(true);
        thread.start();
        log.info("[dispatch] worker {} started", workerId);
    }

    public void stop() {
        running = false;
        log.info("[dispatch] worker {} stopping", workerId);
    }

    @Override
    public void run() {
        long pollMillis = properties.getPollInterval().toMillis();
        while (running) {
            try {
                maybeReclaimStale();
                boolean processed = repository.claimDue(workerId, Instant.now())
                        .map(this::process)
                        .isPresent();
                if (!processed) {
                    sleep(pollMillis);
                }
            } catch (RuntimeException e) {
                log.error("[dispatch] worker {} loop error: {}", workerId, e.getMessage(), e);
                sleep(pollMillis);
            }
        }
    }

    private void maybeReclaimStale() {
        long now = System.currentTimeMillis();
        if (now - lastReclaimMillis < properties.getReclaimInterval().toMillis()) {
            return;
        }
        lastReclaimMillis = now;
        Instant threshold = Instant.now().minus(properties.getVisibilityTimeout());
        int reclaimed = repository.reclaimStale(threshold, Instant.now());
        if (reclaimed > 0) {
            log.warn("[dispatch] worker {} reclaimed {} stale PROCESSING event(s) "
                    + "(visibility timeout {})", workerId, reclaimed,
                    properties.getVisibilityTimeout());
        }
    }

    private boolean process(OutboxEvent event) {
        int attempt = event.getAttempts();
        log.info("[dispatch] eventId={} id={} claimed by worker={} attempt={}/{}",
                event.getEventId(), event.getId(), workerId, attempt,
                properties.getMaxAttempts());
        QueueMessage message = new QueueMessage(
                event.getEventId(),
                event.getAggregateType(),
                event.getAggregateId(),
                event.getEventType().name(),
                event.getPayload(),
                Instant.now());
        try {
            publisher.publish(message);
            // 测试可在此处模拟“副作用已生效但尚未标记 DELIVERED 进程就崩溃”。
            interceptors.beforeMarkDelivered(event);
            if (repository.markDelivered(event.getId(), workerId)) {
                log.info("[dispatch] eventId={} attempt={} verdict=DELIVERED",
                        event.getEventId(), attempt);
            } else {
                log.warn("[dispatch] eventId={} attempt={} verdict=DELIVERED_STALE "
                        + "(lock lost, result left to owner/reclaim)",
                        event.getEventId(), attempt);
            }
            return true;
        } catch (NonRetryableDeliveryException e) {
            boolean dead = repository.markDead(event.getId(), workerId, attempt,
                    e.getMessage(), "NON_RETRYABLE: " + e.getMessage(), Instant.now());
            log.error("[dispatch] eventId={} attempt={} verdict={} reason={}",
                    event.getEventId(), attempt,
                    dead ? "DEAD" : "DEAD_STALE", e.getMessage());
            return true;
        } catch (RetryableDeliveryException e) {
            handleRetryable(event, attempt, e);
            return true;
        } catch (RuntimeException e) {
            handleRetryable(event, attempt,
                    new RetryableDeliveryException("unclassified dispatch error: "
                            + e.getClass().getSimpleName() + ": " + e.getMessage(), e));
            return true;
        }
    }

    private void handleRetryable(OutboxEvent event, int attempt,
                                 RetryableDeliveryException error) {
        if (attempt >= properties.getMaxAttempts()) {
            String reason = "MAX_ATTEMPTS_EXCEEDED after " + attempt + " attempt(s): "
                    + error.getMessage();
            boolean dead = repository.markDead(event.getId(), workerId, attempt,
                    error.getMessage(), reason, Instant.now());
            log.error("[dispatch] eventId={} attempt={} verdict={} lastError={}",
                    event.getEventId(), attempt,
                    dead ? "DEAD" : "DEAD_STALE", error.getMessage());
            return;
        }
        Instant nextAttemptAt = Instant.now().plus(backoffPolicy.nextDelay(attempt));
        boolean scheduled = repository.scheduleRetry(event.getId(), workerId, attempt,
                nextAttemptAt, error.getMessage());
        log.warn("[dispatch] eventId={} attempt={} verdict={} nextAttemptAt={} reason={}",
                event.getEventId(), attempt,
                scheduled ? "RETRY" : "RETRY_STALE", nextAttemptAt, error.getMessage());
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String getWorkerId() {
        return workerId;
    }
}
