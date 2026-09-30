package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.biz.OrderService;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxStatus;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageQueue;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.QueueProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HappyPathTest extends AbstractIntegrationTest {

    @Autowired
    private OrderService orderService;
    @Autowired
    private OutboxRepository repository;
    @Autowired
    private MessageQueue messageQueue;
    @Autowired
    private QueueProperties queueProperties;

    @Test
    void eventIsDeliveredAndSideEffectExecutedExactlyOnce() {
        resumeDispatchers();
        OrderService.CreateOrderResult created = orderService.createOrder("alice");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(repository.findById(created.eventId()).orElseThrow().getStatus())
                    .isEqualTo(OutboxStatus.DELIVERED);
            assertThat(messageQueue.awaitQuiet(queueProperties.queueName(), 1000)).isTrue();
            assertThat(orderService.countFulfillments()).isGreaterThanOrEqualTo(1);
        });

        // broker 重复投递一次：消费端按 eventId 幂等，副作用数量不再增加
        int before = orderService.countFulfillments();
        assertThat(messageQueue.resend(queueProperties.queueName(), created.eventId())).isTrue();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(messageQueue.awaitQuiet(queueProperties.queueName(), 1000)).isTrue());
        assertThat(orderService.countFulfillments()).isEqualTo(before);
    }

    @Test
    void multipleEventsOfSameAggregateStayInOrder() {
        resumeDispatchers();
        OrderService.CreateOrderResult created = orderService.createOrder("bob");
        String shipEventId = orderService.shipOrder(created.orderId());

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(repository.findById(created.eventId()).orElseThrow().getStatus())
                    .isEqualTo(OutboxStatus.DELIVERED);
            assertThat(repository.findById(shipEventId).orElseThrow().getStatus())
                    .isEqualTo(OutboxStatus.DELIVERED);
        });

        var events = repository.findByAggregate(OrderService.AGGREGATE_TYPE, created.orderId());
        assertThat(events).hasSize(2);
        assertThat(events.get(0).getId()).isEqualTo(created.eventId());
        assertThat(events.get(1).getId()).isEqualTo(shipEventId);
    }
}
