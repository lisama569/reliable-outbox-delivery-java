package com.github.lisama569.reliableoutboxdeliveryjava.broker;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 进程内本地消息队列，按真实消息系统的语义来对待：
 * - publish 把消息放入 FIFO 队列后等待消费者确认（类似 broker confirm / ack）；
 * - 每个队列有独立的单线程消费者，按入队顺序串行处理，保证队列内 FIFO；
 * - 消费处理器抛出的可重试 / 不可重试异常会通过确认结果传回发布者，
 *   由 outbox 投递器决定退避重试或死信；
 * - 确认超时按可重试错误处理。
 *
 * 这是内存队列：进程崩溃时队列中尚未确认的消息会丢失，但 outbox_event 表中
 * 的事件仍然存在（停留在 PROCESSING），投递器重启 / 孤儿回收后会重新发布，
 * 仍然满足至少一次投递；重复消息由消费端按 eventId 幂等去重。
 */
@Component
public class LocalMessageBroker implements MessagePublisher {

    public static final String ORDER_EVENTS_QUEUE = "order.events";

    private static final long ACK_TIMEOUT_SECONDS = 10;

    private static final Logger log = LoggerFactory.getLogger(LocalMessageBroker.class);

    private final LinkedBlockingQueue<PendingDelivery> queue = new LinkedBlockingQueue<>();
    private volatile MessageHandler handler;
    private volatile Thread worker;
    private volatile boolean started;

    public synchronized void subscribe(MessageHandler messageHandler) {
        if (started) {
            throw new IllegalStateException("subscriptions are only allowed before start()");
        }
        this.handler = messageHandler;
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        if (handler == null) {
            throw new IllegalStateException("no handler subscribed to queue " + ORDER_EVENTS_QUEUE);
        }
        started = true;
        worker = new Thread(this::deliveryLoop, "local-msg-broker-order-events");
        worker.setDaemon(true);
        worker.start();
        log.info("[broker] local message broker started, queue={}", ORDER_EVENTS_QUEUE);
    }

    /**
     * 发布消息并同步等待消费者确认。
     */
    @Override
    public void publish(QueueMessage message) {
        if (!started) {
            throw new RetryableDeliveryException(
                    "broker is not started, queue=" + ORDER_EVENTS_QUEUE);
        }
        PendingDelivery pending = new PendingDelivery(message);
        queue.add(pending);
        log.info("[broker] enqueued eventId={} onto queue={}", message.getEventId(),
                ORDER_EVENTS_QUEUE);
        try {
            pending.ack.get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RetryableDeliveryException retryable) {
                throw retryable;
            }
            if (cause instanceof NonRetryableDeliveryException nonRetryable) {
                throw nonRetryable;
            }
            throw new RetryableDeliveryException(
                    "unexpected handler failure for eventId=" + message.getEventId(), cause);
        } catch (TimeoutException e) {
            throw new RetryableDeliveryException(
                    "consumer ack timeout for eventId=" + message.getEventId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableDeliveryException(
                    "interrupted while awaiting ack for eventId=" + message.getEventId());
        }
    }

    private void deliveryLoop() {
        while (started || !queue.isEmpty()) {
            try {
                PendingDelivery pending = queue.poll(200, TimeUnit.MILLISECONDS);
                if (pending == null) {
                    continue;
                }
                QueueMessage message = pending.message;
                try {
                    log.info("[broker] delivering eventId={} to handler", message.getEventId());
                    handler.handle(message);
                    pending.ack.complete(null);
                    log.info("[broker] eventId={} acknowledged by handler",
                            message.getEventId());
                } catch (RetryableDeliveryException e) {
                    log.warn("[broker] eventId={} handler failed retryable: {}",
                            message.getEventId(), e.getMessage());
                    pending.ack.completeExceptionally(e);
                } catch (NonRetryableDeliveryException e) {
                    log.error("[broker] eventId={} handler failed non-retryable: {}",
                            message.getEventId(), e.getMessage());
                    pending.ack.completeExceptionally(e);
                } catch (RuntimeException e) {
                    log.warn("[broker] eventId={} handler threw unclassified error, "
                            + "treated as retryable: {}", message.getEventId(), e.getMessage());
                    pending.ack.completeExceptionally(
                            new RetryableDeliveryException("unclassified handler error", e));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public synchronized void reset() {
        queue.clear();
        log.info("[broker] in-memory queue cleared (test support)");
    }

    @PreDestroy
    public synchronized void stop() {
        started = false;
        if (worker != null) {
            worker.interrupt();
        }
    }

    private record PendingDelivery(QueueMessage message, CompletableFuture<Void> ack) {

        private PendingDelivery(QueueMessage message) {
            this(message, new CompletableFuture<>());
        }
    }
}
