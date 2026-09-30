package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.biz.OrderService;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxStatus;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.FailureSimulator;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageQueue;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.QueueProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DuplicateConsumptionTest extends AbstractIntegrationTest {

    @Autowired
    private OrderService orderService;
    @Autowired
    private OutboxRepository repository;
    @Autowired
    private MessageQueue messageQueue;
    @Autowired
    private QueueProperties queueProperties;
    @Autowired
    private FailureSimulator failureSimulator;

    @Test
    void transientConsumeFailuresThenRedeliveryAndBrokerDuplicateYieldSingleSideEffect() {
        OrderService.CreateOrderResult created = orderService.createOrder("erin");
        failureSimulator.failNextConsumesForEvent(created.eventId(), 2);
        resumeDispatchers();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(repository.findById(created.eventId()).orElseThrow().getStatus())
                        .isEqualTo(OutboxStatus.DELIVERED));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(orderService.countFulfillments()).isEqualTo(1));

        // broker 再投递一次（消息系统至少一次语义下的重复）：幂等跳过
        assertThat(messageQueue.resend(queueProperties.queueName(), created.eventId())).isTrue();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(messageQueue.awaitQuiet(queueProperties.queueName(), 1000)).isTrue());
        assertThat(orderService.countFulfillments()).isEqualTo(1);

        // 再来一次 broker 重复投递，结果仍然只有一次副作用
        assertThat(messageQueue.resend(queueProperties.queueName(), created.eventId())).isTrue();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(messageQueue.awaitQuiet(queueProperties.queueName(), 1000)).isTrue());
        assertThat(orderService.countFulfillments()).isEqualTo(1);
    }
}
