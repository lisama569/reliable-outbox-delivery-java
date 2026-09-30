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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 场景四：事件进入死信后，人工重投恢复故障，事件按同一套重试/幂等规则投递成功。
 * 同时覆盖：对非 DEAD 事件重投返回 409、不存在事件返回 404、VIEWER 越权重投返回 403。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = { com.github.lisama569.reliableoutboxdeliveryjava.Application.class,
                TestFailureControl.class })
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:requeue;DB_CLOSE_DELAY=-1",
        "outbox.poll-interval=30ms",
        "outbox.reclaim-interval=200ms",
        "outbox.max-attempts=3",
        "outbox.backoff-initial=30ms",
        "outbox.backoff-max=200ms"
})
class DeadLetterRequeueTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcOutboxRepository outboxRepository;

    @Autowired
    private JdbcConsumerRepository consumerRepository;

    @Autowired
    private TestFailureControl.FailureState failureState;

    @Test
    void deadLetterCanBeRequeuedAndDeliveredAfterRecovery() {
        HttpHeaders ops = TestAuth.ops();

        failureState.failEventsWithPrefix("ORDER_CREATED-ORD-RQ-", 100);

        HttpEntity<Map<String, Object>> createReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-RQ", "amountCents", 7L), ops);
        assertThat(restTemplate.exchange("/api/orders", HttpMethod.POST, createReq, String.class)
                .getStatusCode().value()).isEqualTo(201);

        OutboxEvent dead = await().atMost(Duration.ofSeconds(20)).until(
                () -> outboxRepository.listByStatus(EventStatus.DEAD.name(), 10).stream()
                        .findFirst(),
                java.util.Optional::isPresent).get();
        assertThat(dead.getAttempts()).isEqualTo(3);

        // 运维恢复故障：移除注入的失败，然后人工重投
        failureState.retryFailuresByPrefix.clear();

        ResponseEntity<String> requeueResp = restTemplate.exchange(
                "/api/admin/events/" + dead.getId() + "/requeue",
                HttpMethod.POST, new HttpEntity<>(ops), String.class);
        assertThat(requeueResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(requeueResp.getBody()).contains("\"status\":\"PENDING\"");
        assertThat(requeueResp.getBody()).contains("\"attempts\":0");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(outboxRepository.findById(dead.getId()).orElseThrow().getStatus())
                        .isEqualTo(EventStatus.DELIVERED));

        OutboxEvent delivered = outboxRepository.findById(dead.getId()).orElseThrow();
        assertThat(delivered.getStatus()).isEqualTo(EventStatus.DELIVERED);
        assertThat(delivered.getDeadReason()).isNull();
        assertThat(consumerRepository.projectionCount()).isEqualTo(1);
        assertThat(consumerRepository.consumedCount()).isEqualTo(1);

        // 重复重投：事件已 DELIVERED -> 409 ILLEGAL_STATE
        ResponseEntity<String> secondRequeue = restTemplate.exchange(
                "/api/admin/events/" + dead.getId() + "/requeue",
                HttpMethod.POST, new HttpEntity<>(ops), String.class);
        assertThat(secondRequeue.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(secondRequeue.getBody()).contains("ILLEGAL_STATE");

        // VIEWER 越权重投：403 ACCESS_DENIED
        HttpHeaders viewer = TestAuth.viewer();
        ResponseEntity<String> forbidden = restTemplate.exchange(
                "/api/admin/events/" + dead.getId() + "/requeue",
                HttpMethod.POST, new HttpEntity<>(viewer), String.class);
        assertThat(forbidden.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(forbidden.getBody()).contains("ACCESS_DENIED");

        // 事件不存在：404 RESOURCE_NOT_FOUND
        ResponseEntity<String> notFound = restTemplate.exchange(
                "/api/admin/events/999999/requeue",
                HttpMethod.POST, new HttpEntity<>(ops), String.class);
        assertThat(notFound.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(notFound.getBody()).contains("RESOURCE_NOT_FOUND");

        // 未认证：401
        ResponseEntity<String> unauthenticated = restTemplate.exchange(
                "/api/admin/events/dead-letters",
                HttpMethod.GET, null, String.class);
        assertThat(unauthenticated.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        List<OutboxEvent> deadLetters = outboxRepository.listByStatus(
                EventStatus.DEAD.name(), 50);
        assertThat(deadLetters).isEmpty();
    }
}
