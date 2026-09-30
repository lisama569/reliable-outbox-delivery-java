package com.github.lisama569.reliableoutboxdeliveryjava.queue;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试/演示用故障注入器，模拟消息系统与消费端的各类失败。
 * 生产代码路径同样经过这里，因此失败处理逻辑与真实故障一致。
 */
public class FailureSimulator {

    /** 指定事件的发布前 N 次尝试判定为可重试失败。 */
    private final ConcurrentHashMap<String, AtomicInteger> retryablePublishByEvent =
            new ConcurrentHashMap<>();
    /** 指定事件的前 N 次消费判定为可重试失败。 */
    private final ConcurrentHashMap<String, AtomicInteger> retryableConsumeByEvent =
            new ConcurrentHashMap<>();
    /** 这些事件的 publish 永远是不可重试失败（毒消息）。 */
    private final Set<String> poisonPublishEvents = ConcurrentHashMap.newKeySet();
    /** 全局：接下来 N 次 publish 判定为可重试失败（用于与具体事件无关的场景）。 */
    private final AtomicInteger retryablePublishGlobal = new AtomicInteger();
    /** 可选：放行前需有多少个不同线程到达；达到后永久放行其余事件。 */
    private final Object publishGateLock = new Object();
    private volatile int publishGateParties;
    private final java.util.Set<String> arrivedThreads =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    public PublishOutcome evaluatePublish(String eventId) {
        awaitPublishGate();
        if (poisonPublishEvents.contains(eventId)) {
            return PublishOutcome.nonRetryable("simulated poison event, broker rejects permanently");
        }
        AtomicInteger perEvent = retryablePublishByEvent.get(eventId);
        if (perEvent != null && perEvent.getAndDecrement() > 0) {
            return PublishOutcome.retryable("simulated broker outage");
        }
        if (retryablePublishGlobal.getAndDecrement() > 0) {
            return PublishOutcome.retryable("simulated broker outage");
        }
        return PublishOutcome.success();
    }

    public DeliveryResult evaluateConsume(String eventId) {
        AtomicInteger perEvent = retryableConsumeByEvent.get(eventId);
        if (perEvent != null && perEvent.getAndDecrement() > 0) {
            return DeliveryResult.RETRYABLE_FAILURE;
        }
        return DeliveryResult.SUCCESS;
    }

    /** 让指定事件接下来 n 次发布以可重试方式失败。 */
    public void failNextPublishesForEvent(String eventId, int n) {
        retryablePublishByEvent.computeIfAbsent(eventId, k -> new AtomicInteger()).set(n);
    }

    /** 让指定事件接下来 n 次消费以可重试方式失败（nack 重投）。 */
    public void failNextConsumesForEvent(String eventId, int n) {
        retryableConsumeByEvent.computeIfAbsent(eventId, k -> new AtomicInteger()).set(n);
    }

    /** 让接下来 n 次发布（任意事件）以可重试方式失败。 */
    public void failNextPublishes(int n) {
        retryablePublishGlobal.set(n);
    }

    /** 将指定事件标记为不可重试的毒消息。 */
    public void markPoisonPublish(String eventId) {
        poisonPublishEvents.add(eventId);
    }

    /** 解除毒消息标记（用于人工修复后重投）。 */
    public void unmarkPoisonPublish(String eventId) {
        poisonPublishEvents.remove(eventId);
    }

    public boolean isPoison(String eventId) {
        return poisonPublishEvents.contains(eventId);
    }

    /** 设置发布门闩：需要 parties 个不同发布线程都到达后才放行，之后永久放行。 */
    public void armPublishBarrier(int parties) {
        synchronized (publishGateLock) {
            publishGateParties = parties;
            arrivedThreads.clear();
        }
    }

    public void disarmPublishBarrier() {
        synchronized (publishGateLock) {
            publishGateParties = 0;
            arrivedThreads.clear();
            publishGateLock.notifyAll();
        }
    }

    private void awaitPublishGate() {
        int parties = publishGateParties;
        if (parties <= 0) {
            return;
        }
        synchronized (publishGateLock) {
            arrivedThreads.add(Thread.currentThread().getName());
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (publishGateParties > 0 && arrivedThreads.size() < publishGateParties) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException("publish gate timeout");
                }
                try {
                    java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(
                            publishGateLock, remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("publish gate interrupted", e);
                }
            }
            if (publishGateParties > 0) {
                // 两个不同执行者都已到达：打开门闩，后续事件直接放行
                publishGateParties = 0;
                publishGateLock.notifyAll();
            }
        }
    }

    public void reset() {
        retryablePublishGlobal.set(0);
        retryablePublishByEvent.clear();
        retryableConsumeByEvent.clear();
        poisonPublishEvents.clear();
        publishGateParties = 0;
        arrivedThreads.clear();
    }
}
