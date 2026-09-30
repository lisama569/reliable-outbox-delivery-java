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
 * 场景二：消费端副作用已经提交，但投递器在标记 DELIVERED 之前崩溃，
 * 孤儿回收触发重新投递。重复消息必须被幂等去重，业务副作用只能执行一次。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = { com.github.lisama569.reliableoutboxdeliveryjava.Application.class,
                TestFailureControl.class })
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:dup-consumption;DB_CLOSE_DELAY=-1",
        "outbox.poll-interval=50ms",
        "outbox.reclaim-interval=200ms",
        "outbox.visibility-timeout=1s",
        "outbox.backoff-initial=50ms"
})
class DuplicateConsumptionTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcOutboxRepository outboxRepository;

    @Autowired
    private JdbcConsumerRepository consumerRepository;

    @Autowired
    private TestFailureControl.FailureState failureState;

    @Test
    void crashAfterSideEffectCausesRedeliveryButSideEffectRunsOnce() {
        // 第一次“成功消费后、标记 DELIVERED 前”模拟崩溃 1 次
        failureState.crashBeforeMarkDelivered.set(1);

        HttpHeaders headers = TestAuth.ops();
        HttpEntity<Map<String, Object>> createReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-DUP", "amountCents", 100L), headers);
        ResponseEntity<String> created = restTemplate.exchange(
                "/api/orders", HttpMethod.POST, createReq, String.class);
        assertThat(created.getStatusCode().value()).isEqualTo(201);

        // 最终事件 DELIVERED，且 attempts >= 2（至少经历过一次重复投递）
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            List<OutboxEvent> delivered = outboxRepository.listByStatus(
                    EventStatus.DELIVERED.name(), 10);
            assertThat(delivered).hasSize(1);
            assertThat(delivered.get(0).getAttempts()).isGreaterThanOrEqualTo(2);
        });

        // 读模型副作用只执行了一次（只有一个 order_no 投影）
        assertThat(consumerRepository.projectionCount()).isEqualTo(1);

        OutboxEvent event = outboxRepository.listByStatus(null, 10).get(0);
        assertThat(event.getStatus()).isEqualTo(EventStatus.DELIVERED);
        assertThat(consumerRepository.consumedCount()).isEqualTo(1);
        assertThat(consumerRepository.isConsumed(event.getEventId())).isTrue();
    }
}
