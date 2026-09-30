package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.EventStatus;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 场景七：
 * - 不可重试错误不等待退避、第一次尝试就直接进入死信（deadReason 标记 NON_RETRYABLE）；
 * - 死信可由 OPS 人工标记为已处理（RESOLVED 终态），之后不会再被投递；
 * - 对非 DEAD 事件执行 resolve 返回 409。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = { com.github.lisama569.reliableoutboxdeliveryjava.Application.class,
                TestFailureControl.class })
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:nonretry;DB_CLOSE_DELAY=-1",
        "outbox.poll-interval=30ms",
        "outbox.max-attempts=5",
        "outbox.backoff-initial=500ms"
})
class NonRetryableAndResolveTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcOutboxRepository outboxRepository;

    @Autowired
    private TestFailureControl.FailureState failureState;

    @Test
    void nonRetryableGoesStraightToDeadAndCanBeResolved() {
        HttpHeaders ops = TestAuth.ops();
        failureState.failEventsWithNonRetryPrefix("ORDER_CREATED-ORD-NR-", 100);

        HttpEntity<Map<String, Object>> createReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-NR", "amountCents", 3L), ops);
        assertThat(restTemplate.exchange("/api/orders", HttpMethod.POST, createReq, String.class)
                .getStatusCode().value()).isEqualTo(201);

        OutboxEvent dead = await().atMost(Duration.ofSeconds(15)).until(
                () -> outboxRepository.listByStatus(EventStatus.DEAD.name(), 10).stream()
                        .findFirst(),
                java.util.Optional::isPresent).get();

        assertThat(dead.getAttempts()).isEqualTo(1);
        assertThat(dead.getDeadReason()).startsWith("NON_RETRYABLE:");
        assertThat(dead.getLastError()).contains("INJECTED_NON_RETRYABLE_FAILURE");

        // 等一段时间确认不会因重试而改变（退避初值 500ms、上限 5 次；不可重试不应进入此循环）
        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        OutboxEvent stillDead = outboxRepository.findById(dead.getId()).orElseThrow();
        assertThat(stillDead.getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(stillDead.getAttempts()).isEqualTo(1);

        // OPS 标记为已处理
        ResponseEntity<String> resolveResp = restTemplate.exchange(
                "/api/admin/events/" + dead.getId() + "/resolve",
                HttpMethod.POST, new HttpEntity<>(ops), String.class);
        assertThat(resolveResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resolveResp.getBody()).contains("\"status\":\"RESOLVED\"");

        OutboxEvent resolved = outboxRepository.findById(dead.getId()).orElseThrow();
        assertThat(resolved.getStatus()).isEqualTo(EventStatus.RESOLVED);
        assertThat(resolved.getLockedBy()).isNull();

        // 已 RESOLVED 的事件再次 resolve / requeue 都是非法状态流转 -> 409
        ResponseEntity<String> again = restTemplate.exchange(
                "/api/admin/events/" + dead.getId() + "/resolve",
                HttpMethod.POST, new HttpEntity<>(ops), String.class);
        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(again.getBody()).contains("ILLEGAL_STATE");

        ResponseEntity<String> requeueResolved = restTemplate.exchange(
                "/api/admin/events/" + dead.getId() + "/requeue",
                HttpMethod.POST, new HttpEntity<>(ops), String.class);
        assertThat(requeueResolved.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        // VIEWER 无权 resolve -> 403
        HttpHeaders viewer = TestAuth.viewer();
        ResponseEntity<String> forbidden = restTemplate.exchange(
                "/api/admin/events/" + dead.getId() + "/resolve",
                HttpMethod.POST, new HttpEntity<>(viewer), String.class);
        assertThat(forbidden.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }
}
