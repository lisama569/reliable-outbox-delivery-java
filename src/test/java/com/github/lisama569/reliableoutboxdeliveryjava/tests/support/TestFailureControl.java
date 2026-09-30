package com.github.lisama569.reliableoutboxdeliveryjava.tests.support;

import com.github.lisama569.reliableoutboxdeliveryjava.broker.NonRetryableDeliveryException;
import com.github.lisama569.reliableoutboxdeliveryjava.broker.RetryableDeliveryException;
import com.github.lisama569.reliableoutboxdeliveryjava.consumer.ConsumerFailureHook;
import com.github.lisama569.reliableoutboxdeliveryjava.dispatch.DispatchInterceptors;
import com.github.lisama569.reliableoutboxdeliveryjava.dispatch.ProcessKilledSimulated;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试故障注入中心：
 * - 对指定 eventId 的前 N 次消费注入可重试/不可重试异常；
 * - 在“消费已成功、但标记 DELIVERED 之前”抛出异常，模拟进程崩溃导致的重复投递。
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestFailureControl {

    private static final Logger log = LoggerFactory.getLogger(TestFailureControl.class);

    @Bean
    public FailureState failureState() {
        return new FailureState();
    }

    @Bean
    public DispatchInterceptors crashInterceptors(FailureState state) {
        return new DispatchInterceptors() {
            @Override
            public void beforeMarkDelivered(OutboxEvent event) {
                int remaining = state.crashBeforeMarkDelivered.getAndDecrement();
                if (remaining > 0) {
                    log.warn("[test-fault] eventId={} simulated process KILL after consumer "
                            + "side-effect but BEFORE DELIVERED mark; dispatcher thread "
                            + "terminates now", event.getEventId());
                    throw new ProcessKilledSimulated(
                            "SIMULATED_PROCESS_KILL_BEFORE_DELIVERED_MARK eventId="
                                    + event.getEventId());
                }
            }
        };
    }

    /**
     * 模拟进程崩溃的标记错误，可重试。
     */
    public static class SimulatedProcessCrash extends RetryableDeliveryException {
        public SimulatedProcessCrash() {
            super("SIMULATED_PROCESS_CRASH");
        }
    }

    public static class FailureState implements ConsumerFailureHook {

        /** 剩余的“标记 DELIVERED 前崩溃”次数，>0 时生效。 */
        public final AtomicInteger crashBeforeMarkDelivered = new AtomicInteger(0);

        /** eventId -> 前多少次消费注入可重试失败。 */
        public final ConcurrentMap<String, Integer> retryFailures = new ConcurrentHashMap<>();

        /** eventId -> 前多少次消费注入不可重试失败。 */
        public final ConcurrentMap<String, Integer> nonRetryFailures = new ConcurrentHashMap<>();

        /** eventId 前缀 -> 前多少次消费注入可重试失败。 */
        public final ConcurrentMap<String, Integer> retryFailuresByPrefix =
                new ConcurrentHashMap<>();

        /** eventId 前缀 -> 前多少次消费注入不可重试失败。 */
        public final ConcurrentMap<String, Integer> nonRetryFailuresByPrefix =
                new ConcurrentHashMap<>();

        public final AtomicInteger sideEffectCount = new AtomicInteger(0);

        @Override
        public void checkDelivery(String eventId, int deliveryIndex) {
            Integer retryUntil = retryFailures.get(eventId);
            if (retryUntil != null && deliveryIndex <= retryUntil) {
                log.warn("[test-fault] eventId={} deliveryIndex={} injected RETRYABLE failure",
                        eventId, deliveryIndex);
                throw new RetryableDeliveryException(
                        "INJECTED_RETRYABLE_FAILURE#" + deliveryIndex);
            }
            for (var entry : retryFailuresByPrefix.entrySet()) {
                if (eventId.startsWith(entry.getKey()) && deliveryIndex <= entry.getValue()) {
                    log.warn("[test-fault] eventId={} deliveryIndex={} injected RETRYABLE "
                            + "failure (prefix rule)", eventId, deliveryIndex);
                    throw new RetryableDeliveryException(
                            "INJECTED_RETRYABLE_FAILURE#" + deliveryIndex);
                }
            }
            for (var entry : nonRetryFailuresByPrefix.entrySet()) {
                if (eventId.startsWith(entry.getKey()) && deliveryIndex <= entry.getValue()) {
                    log.error("[test-fault] eventId={} deliveryIndex={} injected NON-RETRYABLE "
                            + "failure (prefix rule)", eventId, deliveryIndex);
                    throw new NonRetryableDeliveryException(
                            "INJECTED_NON_RETRYABLE_FAILURE#" + deliveryIndex);
                }
            }
            Integer nonRetryUntil = nonRetryFailures.get(eventId);
            if (nonRetryUntil != null && deliveryIndex <= nonRetryUntil) {
                log.error("[test-fault] eventId={} deliveryIndex={} injected NON-RETRYABLE failure",
                        eventId, deliveryIndex);
                throw new NonRetryableDeliveryException(
                        "INJECTED_NON_RETRYABLE_FAILURE#" + deliveryIndex);
            }
        }

        public void failFirstNWithRetry(String eventId, int n) {
            retryFailures.put(eventId, n);
        }

        public void failEventsWithPrefix(String eventIdPrefix, int n) {
            retryFailuresByPrefix.put(eventIdPrefix, n);
        }

        public void failEventsWithNonRetryPrefix(String eventIdPrefix, int n) {
            nonRetryFailuresByPrefix.put(eventIdPrefix, n);
        }

        public void failFirstNWithNonRetry(String eventId, int n) {
            nonRetryFailures.put(eventId, n);
        }

        public void reset() {
            crashBeforeMarkDelivered.set(0);
            retryFailures.clear();
            nonRetryFailures.clear();
        }
    }
}
