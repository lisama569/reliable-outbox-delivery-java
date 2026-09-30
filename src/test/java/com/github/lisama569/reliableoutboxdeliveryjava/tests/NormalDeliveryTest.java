package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcConsumerRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.tests.support.TestAuth;
import com.github.lisama569.reliableoutboxdeliveryjava.tests.support.TestFailureControl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 场景一：订单创建并支付，事件最终 DELIVERED，消费端读模型恰好更新一次。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = { com.github.lisama569.reliableoutboxdeliveryjava.Application.class,
                TestFailureControl.class })
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:normal-delivery;DB_CLOSE_DELAY=-1",
        "outbox.poll-interval=50ms",
        "outbox.reclaim-interval=200ms"
})
class NormalDeliveryTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcOutboxRepository outboxRepository;

    @Autowired
    private JdbcConsumerRepository consumerRepository;

    @Test
    void orderLifecycleEventsAreDeliveredAndConsumedOnce() {
        HttpHeaders headers = TestAuth.ops();
        HttpEntity<Map<String, Object>> createReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-1001", "amountCents", 2500L), headers);
        ResponseEntity<String> created = restTemplate.exchange(
                "/api/orders", HttpMethod.POST, createReq, String.class);
        assertThat(created.getStatusCode().value()).isEqualTo(201);

        HttpEntity<Map<String, Object>> payReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-1001"), headers);
        ResponseEntity<String> paid = restTemplate.exchange(
                "/api/orders/payments", HttpMethod.POST, payReq, String.class);
        assertThat(paid.getStatusCode().is2xxSuccessful()).isTrue();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<OutboxEvent> events = outboxRepository.listByStatus(null, 50);
            assertThat(events).hasSize(2);
            assertThat(events).extracting(OutboxEvent::getStatus)
                    .allMatch(s -> s.name().equals("DELIVERED"));
        });

        await().untilAsserted(() ->
                assertThat(consumerRepository.projectionCount()).isEqualTo(1));

        List<OutboxEvent> events = outboxRepository.listByStatus(null, 50);
        assertThat(events).extracting(OutboxEvent::getAggregateId)
                .containsOnly("ORD-1001");
        assertThat(events).extracting(OutboxEvent::getEventType)
                .extracting(Enum::name)
                .containsExactlyInAnyOrder("ORDER_CREATED", "ORDER_PAID");
    }
}
