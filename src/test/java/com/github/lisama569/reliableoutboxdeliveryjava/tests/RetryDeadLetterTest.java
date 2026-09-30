package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.biz.OrderService;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxStatus;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.FailureSimulator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RetryDeadLetterTest extends AbstractIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private OrderService orderService;
    @Autowired
    private OutboxRepository repository;
    @Autowired
    private FailureSimulator failureSimulator;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static HttpHeaders basic(String user, String password) {
        HttpHeaders headers = new HttpHeaders();
        String token = Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        headers.set("Authorization", "Basic " + token);
        return headers;
    }

    @Test
    void retryableFailuresExhaustAttemptsThenRedriveSucceeds() {
        OrderService.CreateOrderResult created = orderService.createOrder("carol");
        // 投递器已暂停：注入在首次轮询前完成，消除时序竞态
        failureSimulator.failNextPublishesForEvent(created.eventId(), 100);
        resumeDispatchers();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(repository.findById(created.eventId()).orElseThrow().getStatus())
                        .isEqualTo(OutboxStatus.DEAD));

        var dead = repository.findById(created.eventId()).orElseThrow();
        assertThat(dead.getAttemptCount()).isEqualTo(dead.getMaxAttempts());
        assertThat(dead.getDeadReason()).contains("retry limit");
        assertThat(dead.getLastError()).contains("simulated broker outage");
        assertThat(dead.getLockedBy()).isNull();

        // 修复故障后人工重投：遵循同一套规则，这次成功
        failureSimulator.failNextPublishesForEvent(created.eventId(), 0);
        ResponseEntity<String> redrive = restTemplate.exchange(
                url("/api/admin/events/" + created.eventId() + "/redrive"),
                HttpMethod.POST, new HttpEntity<>(basic("admin", "admin123")), String.class);
        assertThat(redrive.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(redrive.getBody()).contains("\"status\":\"PENDING\"");
        assertThat(redrive.getBody()).contains("\"redeliveryCount\":1");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(repository.findById(created.eventId()).orElseThrow().getStatus())
                        .isEqualTo(OutboxStatus.DELIVERED));
        // 重投后开启新的一轮计数（不再沿用死信前的 5 次）；成功后应为 1
        assertThat(repository.findById(created.eventId()).orElseThrow().getAttemptCount())
                .isEqualTo(1);
    }

    @Test
    void poisonEventGoesStraightToDeadAndCanBeResolved() {
        OrderService.CreateOrderResult created = orderService.createOrder("dave");
        failureSimulator.markPoisonPublish(created.eventId());
        resumeDispatchers();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(repository.findById(created.eventId()).orElseThrow().getStatus())
                        .isEqualTo(OutboxStatus.DEAD));

        var dead = repository.findById(created.eventId()).orElseThrow();
        assertThat(dead.getAttemptCount()).isEqualTo(1);
        assertThat(dead.getDeadReason()).contains("non-retryable");

        ResponseEntity<String> resolve = restTemplate.exchange(
                url("/api/admin/events/" + created.eventId() + "/resolve"),
                HttpMethod.POST, new HttpEntity<>(basic("admin", "admin123")), String.class);
        assertThat(resolve.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resolve.getBody()).contains("\"status\":\"RESOLVED\"");
        assertThat(repository.findById(created.eventId()).orElseThrow().getStatus())
                .isEqualTo(OutboxStatus.RESOLVED);
    }
}
