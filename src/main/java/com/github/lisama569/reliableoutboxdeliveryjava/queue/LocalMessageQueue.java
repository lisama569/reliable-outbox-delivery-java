package com.github.lisama569.reliableoutboxdeliveryjava.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内本地消息队列：有独立工作线程、拉取、ack/nack 与延迟重投，
 * 按真实消息系统的至少一次语义对待。不做持久化（持久化语义由 outbox 表保证）。
 */
public class LocalMessageQueue implements MessageQueue {

    private static final Logger log = LoggerFactory.getLogger(LocalMessageQueue.class);

    private final QueueProperties properties;
    private final FailureSimulator failureSimulator;

    private final Map<String, ConcurrentLinkedQueue<Message>> queues = new ConcurrentHashMap<>();
    private final Map<String, MessageHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, List<Message>> inFlightByQueue = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedQueue<String>> queueDeadLetters = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Message>> lastDeliveredByQueue = new ConcurrentHashMap<>();

    private final List<Thread> workers = new ArrayList<>();
    private final ScheduledExecutorService redeliverer =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mq-redeliverer");
                t.setDaemon(true);
                return t;
            });
    private final AtomicLong deliverySeq = new AtomicLong();
    private volatile boolean running;

    public LocalMessageQueue(QueueProperties properties, FailureSimulator failureSimulator) {
        this.properties = properties;
        this.failureSimulator = failureSimulator;
    }

    @PostConstruct
    public void start() {
        running = true;
        for (int i = 0; i < properties.consumerWorkers(); i++) {
            Thread worker = new Thread(this::workerLoop, "mq-worker-" + i);
            worker.setDaemon(true);
            worker.start();
            workers.add(worker);
        }
        log.info("LocalMessageQueue started, workers={}, redeliveryDelayMillis={}",
                properties.consumerWorkers(), properties.redeliveryDelayMillis());
    }

    @PreDestroy
    public void stop() {
        running = false;
        redeliverer.shutdownNow();
        for (Thread worker : workers) {
            worker.interrupt();
        }
    }

    @Override
    public PublishOutcome publish(String queueName, String eventId, byte[] payload) {
        PublishOutcome outcome = failureSimulator.evaluatePublish(eventId);
        if (!outcome.succeeded()) {
            log.info("publish rejected eventId={} result={} error={}", eventId, outcome.result(),
                    outcome.error());
            return outcome;
        }
        Message message = new Message("dlv-" + deliverySeq.incrementAndGet(), queueName,
                eventId, payload, 0);
        ensureQueue(queueName).add(message);
        log.info("publish accepted eventId={} queue={}", eventId, queueName);
        return outcome;
    }

    @Override
    public void subscribe(String queueName, MessageHandler handler) {
        ensureQueue(queueName);
        handlers.put(queueName, handler);
        log.info("handler subscribed queue={}", queueName);
    }

    @Override
    public boolean resend(String queueName, String eventId) {
        Map<String, Message> lastDelivered = lastDeliveredByQueue.get(queueName);
        if (lastDelivered == null) {
            return false;
        }
        Message original = lastDelivered.get(eventId);
        if (original == null) {
            return false;
        }
        Message redelivery = original.nextRedelivery("dlv-" + deliverySeq.incrementAndGet());
        queues.get(queueName).add(redelivery);
        log.info("broker redelivery scheduled eventId={} queue={} deliveryCount={}",
                eventId, queueName, redelivery.getDeliveryCount());
        return true;
    }

    @Override
    public List<String> queueDeadLetterIds(String queueName) {
        ConcurrentLinkedQueue<String> dead = queueDeadLetters.get(queueName);
        return dead == null ? List.of() : List.copyOf(dead);
    }

    @Override
    public synchronized void clearAll() {
        queues.values().forEach(java.util.Collection::clear);
        inFlightByQueue.values().forEach(l -> {
            synchronized (l) {
                l.clear();
            }
        });
        queueDeadLetters.values().forEach(java.util.Collection::clear);
        lastDeliveredByQueue.values().forEach(java.util.Map::clear);
        log.info("LocalMessageQueue state cleared for test reset");
    }

    @Override
    public boolean awaitQuiet(String queueName, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            ConcurrentLinkedQueue<Message> queue = queues.get(queueName);
            List<Message> inFlight = inFlightByQueue.get(queueName);
            if ((queue == null || queue.isEmpty()) && (inFlight == null || inFlight.isEmpty())) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    private void workerLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            boolean processed = false;
            for (Map.Entry<String, ConcurrentLinkedQueue<Message>> entry : queues.entrySet()) {
                String queueName = entry.getKey();
                MessageHandler handler = handlers.get(queueName);
                Message message = entry.getValue().poll();
                if (message == null || handler == null) {
                    if (message != null) {
                        entry.getValue().add(message);
                    }
                    continue;
                }
                processed = true;
                deliver(queueName, handler, message);
            }
            if (!processed) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void deliver(String queueName, MessageHandler handler, Message message) {
        message.incrementDeliveryCount();
        inFlight(queueName).add(message);
        DeliveryResult result;
        try {
            result = handler.handle(message);
        } catch (Exception e) {
            log.warn("handler threw eventId={} deliveryCount={} error={}",
                    message.getEventId(), message.getDeliveryCount(), e.toString());
            result = DeliveryResult.RETRYABLE_FAILURE;
        }
        inFlight(queueName).remove(message);

        switch (result) {
            case SUCCESS -> {
                lastDelivered(queueName).put(message.getEventId(), message);
                log.info("message acked eventId={} deliveryCount={}",
                        message.getEventId(), message.getDeliveryCount());
            }
            case RETRYABLE_FAILURE -> {
                log.info("message nacked eventId={} deliveryCount={}, will redeliver in {}ms",
                        message.getEventId(), message.getDeliveryCount(),
                        properties.redeliveryDelayMillis());
                redeliverer.schedule(() -> {
                    Message next = message.nextRedelivery("dlv-" + deliverySeq.incrementAndGet());
                    ensureQueue(queueName).add(next);
                }, properties.redeliveryDelayMillis(), TimeUnit.MILLISECONDS);
            }
            case NON_RETRYABLE_FAILURE -> {
                queueDeadLetters.computeIfAbsent(queueName, k -> new ConcurrentLinkedQueue<>())
                        .add(message.getEventId());
                log.warn("message rejected (queue DLQ) eventId={} deliveryCount={}",
                        message.getEventId(), message.getDeliveryCount());
            }
        }
    }

    private ConcurrentLinkedQueue<Message> ensureQueue(String queueName) {
        return queues.computeIfAbsent(queueName, k -> {
            inFlightByQueue.put(k, java.util.Collections.synchronizedList(new ArrayList<>()));
            lastDeliveredByQueue.put(k, new ConcurrentHashMap<>());
            return new ConcurrentLinkedQueue<>();
        });
    }

    private List<Message> inFlight(String queueName) {
        return inFlightByQueue.computeIfAbsent(queueName, k ->
                java.util.Collections.synchronizedList(new ArrayList<>()));
    }

    private Map<String, Message> lastDelivered(String queueName) {
        return lastDeliveredByQueue.computeIfAbsent(queueName, k -> new ConcurrentHashMap<>());
    }
}
