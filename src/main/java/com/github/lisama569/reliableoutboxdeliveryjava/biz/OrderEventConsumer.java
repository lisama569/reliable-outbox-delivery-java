package com.github.lisama569.reliableoutboxdeliveryjava.biz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.DeliveryResult;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.FailureSimulator;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.Message;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageHandler;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageQueue;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.QueueProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单事件消费端：按事件标识幂等处理，重复投递不会重复执行副作用；
 * 处理失败返回 RETRYABLE_FAILURE，由队列按至少一次语义重投。
 */
@Component
public class OrderEventConsumer implements MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final MessageQueue messageQueue;
    private final QueueProperties queueProperties;
    private final FulfillmentService fulfillmentService;
    private final FailureSimulator failureSimulator;
    private final ObjectMapper objectMapper;

    public OrderEventConsumer(MessageQueue messageQueue, QueueProperties queueProperties,
                              FulfillmentService fulfillmentService,
                              FailureSimulator failureSimulator, ObjectMapper objectMapper) {
        this.messageQueue = messageQueue;
        this.queueProperties = queueProperties;
        this.fulfillmentService = fulfillmentService;
        this.failureSimulator = failureSimulator;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void subscribe() {
        messageQueue.subscribe(queueProperties.queueName(), this);
    }

    @Override
    public DeliveryResult handle(Message message) {
        String eventId = message.getEventId();
        try {
            JsonNode node = objectMapper.readTree(message.getPayload());
            String eventType = node.path("eventType").asText();

            DeliveryResult injected = failureSimulator.evaluateConsume(eventId);
            if (injected == DeliveryResult.RETRYABLE_FAILURE) {
                log.info("eventId={} consume attempt {} verdict=RETRYABLE (injected transient failure)",
                        eventId, message.getDeliveryCount());
                return DeliveryResult.RETRYABLE_FAILURE;
            }

            switch (eventType) {
                case OrderService.EVENT_ORDER_CREATED -> {
                    String orderId = node.path("orderId").asText();
                    String customer = node.path("customer").asText();
                    boolean firstTime =
                            fulfillmentService.fulfillOrderIfFirstTime(eventId, orderId, customer);
                    log.info("eventId={} consume attempt {} verdict=SUCCESS action={} orderId={}",
                            eventId, message.getDeliveryCount(),
                            firstTime ? "FULFILLED" : "SKIPPED_DUPLICATE", orderId);
                }
                case OrderService.EVENT_ORDER_SHIPPED -> {
                    boolean firstTime = fulfillmentService.markProcessedIfFirstTime(eventId);
                    log.info("eventId={} consume attempt {} verdict=SUCCESS action={}",
                            eventId, message.getDeliveryCount(),
                            firstTime ? "SHIP_RECORDED" : "SKIPPED_DUPLICATE");
                }
                default -> {
                    log.error("eventId={} unknown event type {}", eventId, eventType);
                    return DeliveryResult.NON_RETRYABLE_FAILURE;
                }
            }
            return DeliveryResult.SUCCESS;
        } catch (Exception e) {
            log.warn("eventId={} consume attempt {} threw: {}", eventId,
                    message.getDeliveryCount(), e.toString());
            return DeliveryResult.RETRYABLE_FAILURE;
        }
    }
}
