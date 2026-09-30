package com.github.lisama569.reliableoutboxdeliveryjava.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliableoutboxdeliveryjava.broker.NonRetryableDeliveryException;
import com.github.lisama569.reliableoutboxdeliveryjava.broker.QueueMessage;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcConsumerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * order.events 队列的消费者。
 * 业务副作用 = 更新读模型 order_projection；按 eventId 做幂等，
 * 重复投递（至少一次语义下必然可能发生）不会把副作用执行两次。
 */
@Component
public class OrderEventHandler implements com.github.lisama569.reliableoutboxdeliveryjava.broker.MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);

    private final JdbcConsumerRepository consumerRepository;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, AtomicInteger> deliveryCounters = new ConcurrentHashMap<>();

    private volatile ConsumerFailureHook failureHook;

    public OrderEventHandler(JdbcConsumerRepository consumerRepository,
                             ObjectMapper objectMapper) {
        this.consumerRepository = consumerRepository;
        this.objectMapper = objectMapper;
    }

    public void setFailureHook(ConsumerFailureHook failureHook) {
        this.failureHook = failureHook;
    }

    @Override
    public void handle(QueueMessage message) {
        String eventId = message.getEventId();
        int deliveryIndex = deliveryCounters
                .computeIfAbsent(eventId, k -> new AtomicInteger())
                .incrementAndGet();
        log.info("[consumer] eventId={} deliveryIndex={} type={} received",
                eventId, deliveryIndex, message.getEventType());

        if (failureHook != null) {
            failureHook.checkDelivery(eventId, deliveryIndex);
        }

        String orderNo = extractOrderNo(message.getPayload());
        String status = "ORDER_PAID".equals(message.getEventType()) ? "PAID" : "CREATED";
        boolean firstTime = consumerRepository.consumeIfFirstTime(
                eventId, orderNo, message.getEventType(), status);
        log.info("[consumer] eventId={} deliveryIndex={} verdict={}",
                eventId, deliveryIndex, firstTime ? "SIDE_EFFECT_APPLIED" : "DUPLICATE_SKIPPED");
    }

    private String extractOrderNo(String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            String orderNo = node.path("orderNo").asText();
            if (orderNo == null || orderNo.isBlank()) {
                throw new NonRetryableDeliveryException(
                        "payload missing orderNo: " + payload);
            }
            return orderNo;
        } catch (NonRetryableDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new NonRetryableDeliveryException("invalid payload: " + payload, e);
        }
    }
}
