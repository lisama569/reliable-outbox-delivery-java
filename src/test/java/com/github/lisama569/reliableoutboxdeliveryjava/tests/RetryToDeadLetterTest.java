package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.EventStatus;
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
 * 场景三：消费端持续返回可重试错误，投递器按指数退避重试，
 * 达到尝试上限后进入死信，并保留失败原因与尝试次数。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = { com.github.lisama569.reliableoutboxdeliveryjava.Application.class,
                TestFailureControl.class })
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:retry-dead;DB_CLOSE_DELAY=-1",
        "outbox.poll-interval=30ms",
        "outbox.reclaim-interval=200ms",
        "outbox.max-attempts=5",
        "outbox.backoff-initial=30ms",
        "outbox.backoff-multiplier=2.0",
        "outbox.backoff-max=200ms"
})
class RetryToDeadLetterTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcOutboxRepository outboxRepository;

    @Autowired
    private JdbcConsumerRepository consumerRepository;

    @Autowired
    private TestFailureControl.FailureState failureState;

    @Test
    void persistentRetryableFailureEndsInDeadLetterWithReasonAndAttempts() {
        HttpHeaders headers = TestAuth.ops();

        // 对该订单的创建事件预先注入“永久可重试失败”（远超尝试上限 5）
        String expectedEventPrefix = "ORDER_CREATED-ORD-RETRY-";
        failureState.failEventsWithPrefix(expectedEventPrefix, 100);

        HttpEntity<Map<String, Object>> createReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-RETRY", "amountCents", 99L), headers);
        ResponseEntity<String> created = restTemplate.exchange(
                "/api/orders", HttpMethod.POST, createReq, String.class);
        assertThat(created.getStatusCode().value()).isEqualTo(201);

        OutboxEvent event = await().atMost(Duration.ofSeconds(5)).until(
                () -> outboxRepository.listByStatus(null, 10).stream().findFirst(),
                java.util.Optional::isPresent).get();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(outboxRepository.findByEventId(event.getEventId())
                        .orElseThrow().getStatus()).isEqualTo(EventStatus.DEAD));

        OutboxEvent dead = outboxRepository.findByEventId(event.getEventId()).orElseThrow();
        assertThat(dead.getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(dead.getAttempts()).isEqualTo(5);
        assertThat(dead.getLastError()).contains("INJECTED_RETRYABLE_FAILURE");
        assertThat(dead.getDeadReason()).contains("MAX_ATTEMPTS_EXCEEDED");
        assertThat(dead.getDeadAt()).isNotNull();

        // 副作用从未执行（消费在去重记录插入之前就失败了）
        assertThat(consumerRepository.projectionCount()).isZero();
        assertThat(consumerRepository.consumedCount()).isZero();
        assertThat(event.getEventId()).startsWith(expectedEventPrefix);

        // 死信列表可见
        List<OutboxEvent> deadLetters = outboxRepository.listByStatus(
                EventStatus.DEAD.name(), 50);
        assertThat(deadLetters).hasSize(1);
    }
}
