package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

import com.github.lisama569.reliableoutboxdeliveryjava.queue.DeliveryResult;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageQueue;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.PublishOutcome;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.QueueProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 后台投递器：把 outbox 表中的 PENDING 事件发布到消息队列。
 * 多个投递器实例并发运行，靠数据库条件 UPDATE 竞争，同一条事件不会被重复领取。
 */
public class OutboxDispatcher {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final String dispatcherId;
    private final OutboxProperties properties;
    private final OutboxRepository repository;
    private final MessageQueue messageQueue;
    private final QueueProperties queueProperties;
    private final JdbcTemplate jdbcTemplate;

    private final AtomicBoolean pollingPaused = new AtomicBoolean();

    public OutboxDispatcher(String dispatcherId, OutboxProperties properties,
                            OutboxRepository repository, MessageQueue messageQueue,
                            QueueProperties queueProperties, JdbcTemplate jdbcTemplate) {
        this.dispatcherId = dispatcherId;
        this.properties = properties;
        this.repository = repository;
        this.messageQueue = messageQueue;
        this.queueProperties = queueProperties;
        this.jdbcTemplate = jdbcTemplate;
    }

    private ScheduledExecutorService pollExecutor;
    private ScheduledExecutorService recoveryExecutor;

    public void start() {
        pollExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-poll-" + dispatcherId);
            t.setDaemon(true);
            return t;
        });
        // 回收必须有独立线程：领取/投递可能阻塞（行锁等待、屏障等），
        // 若与轮询共用线程，崩溃恢复会被饿死。
        recoveryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-recover-" + dispatcherId);
            t.setDaemon(true);
            return t;
        });
        pollExecutor.scheduleWithFixedDelay(this::safePoll,
                properties.pollIntervalMillis(), properties.pollIntervalMillis(),
                TimeUnit.MILLISECONDS);
        recoveryExecutor.scheduleWithFixedDelay(this::safeRecover,
                properties.recoveryIntervalMillis(), properties.recoveryIntervalMillis(),
                TimeUnit.MILLISECONDS);
        log.info("outbox dispatcher started id={} maxAttempts={}", dispatcherId,
                properties.defaultMaxAttempts());
    }

    public void stop() {
        if (pollExecutor != null) {
            pollExecutor.shutdownNow();
        }
        if (recoveryExecutor != null) {
            recoveryExecutor.shutdownNow();
        }
    }

    private void safePoll() {
        if (pollingPaused.get()) {
            return;
        }
        try {
            poll();
        } catch (Exception e) {
            log.error("dispatcher {} poll failed", dispatcherId, e);
        }
    }

    /** 暂停领取/投递（崩溃回收仍独立运行）。 */
    public void pausePolling() {
        pollingPaused.set(true);
        log.info("dispatcher {} polling paused (recovery still active)", dispatcherId);
    }

    public void resumePolling() {
        pollingPaused.set(false);
        log.info("dispatcher {} polling resumed", dispatcherId);
    }


    private void safeRecover() {
        try {
            Instant cutoff = Instant.now().minusMillis(properties.lockTimeoutMillis());
            int staleCount = repository.countStaleInProgress(cutoff);
            int recovered = repository.recoverStaleLocks(dispatcherId, cutoff);
            if (staleCount > 0 || recovered > 0) {
                log.info("dispatcher {} recovery sweep: staleCandidates={} recovered={} cutoff={}",
                        dispatcherId, staleCount, recovered, cutoff);
            }
            if (recovered > 0) {
                log.warn("dispatcher {} recovered {} stale IN_PROGRESS events after crash/lock timeout",
                        dispatcherId, recovered);
            }
        } catch (Exception e) {
            log.error("dispatcher {} recovery failed", dispatcherId, e);
        }
    }

    void poll() {
        List<OutboxEvent> claimed =
                repository.claimBatch(dispatcherId, properties.pollBatchSize());
        for (OutboxEvent event : claimed) {
            try {
                deliver(event);
            } catch (Exception e) {
                handleFailure(event, DeliveryResult.RETRYABLE_FAILURE,
                        "unexpected error: " + e);
            }
        }
    }

    private void deliver(OutboxEvent event) {
        int attempt = event.getAttemptCount() + 1;

        // 审计：每条事件每次被某投递器处理都落一行，配合领取锁可验证无重复处理
        jdbcTemplate.update("""
                INSERT INTO dispatch_audit (event_id, dispatcher_id, attempt_no, processed_at)
                VALUES (?, ?, ?, ?)
                """, event.getId(), dispatcherId, attempt, Timestamp.from(Instant.now()));

        PublishOutcome outcome;
        try {
            outcome = messageQueue.publish(queueProperties.queueName(), event.getId(),
                    event.getPayload().getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            handleFailure(event, DeliveryResult.RETRYABLE_FAILURE,
                    "publish threw: " + e);
            return;
        }

        if (outcome.succeeded()) {
            repository.markDelivered(event.getId(), attempt);
            log.info("eventId={} attempt={} verdict=SUCCESS status=DELIVERED dispatcher={}",
                    event.getId(), attempt, dispatcherId);
            return;
        }
        handleFailure(event, outcome.result(), outcome.error());
    }

    private void handleFailure(OutboxEvent event, DeliveryResult result, String error) {
        int attempt = event.getAttemptCount() + 1;
        int maxAttempts = event.getMaxAttempts();

        if (result == DeliveryResult.NON_RETRYABLE_FAILURE) {
            String reason = "non-retryable failure: " + error;
            repository.markDead(event.getId(), attempt, reason, error);
            log.warn("eventId={} attempt={} verdict=NON_RETRYABLE_FAILURE status=DEAD reason={}",
                    event.getId(), attempt, reason);
            return;
        }

        if (attempt >= maxAttempts) {
            String reason = "retry limit (" + maxAttempts + ") exhausted, last error: " + error;
            repository.markDead(event.getId(), attempt, reason, error);
            log.warn("eventId={} attempt={}/{} verdict=RETRYABLE_FAILURE status=DEAD reason={}",
                    event.getId(), attempt, maxAttempts, reason);
            return;
        }

        long backoff = properties.backoffMillisFor(attempt);
        Instant nextRetryAt = Instant.now().plusMillis(backoff);
        repository.markRetryPending(event.getId(), attempt, nextRetryAt, error);
        log.info("eventId={} attempt={}/{} verdict=RETRYABLE_FAILURE status=PENDING "
                        + "nextRetryAt=+{}ms dispatcher={}",
                event.getId(), attempt, maxAttempts, backoff, dispatcherId);
    }

    public String getDispatcherId() {
        return dispatcherId;
    }
}
