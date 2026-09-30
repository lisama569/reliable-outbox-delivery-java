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
class AuthorizationAndErrorsTest extends AbstractIntegrationTest {

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

    private String createDeliveredEventId() {
        String eventId = orderService.createOrder("frank").eventId();
        resumeDispatchers();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(repository.findById(eventId).orElseThrow().getStatus())
                        .isEqualTo(OutboxStatus.DELIVERED));
        return eventId;
    }

    private String createDeadEventId() {
        String eventId = orderService.createOrder("grace").eventId();
        failureSimulator.markPoisonPublish(eventId);
        resumeDispatchers();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(repository.findById(eventId).orElseThrow().getStatus())
                        .isEqualTo(OutboxStatus.DEAD));
        return eventId;
    }

    @Test
    void viewerCanQueryButCannotRedrive() {
        String eventId = createDeliveredEventId();

        ResponseEntity<String> query = restTemplate.exchange(
                url("/api/admin/events/" + eventId), HttpMethod.GET,
                new HttpEntity<>(basic("viewer", "viewer123")), String.class);
        assertThat(query.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(query.getBody()).contains(eventId);

        ResponseEntity<String> redrive = restTemplate.exchange(
                url("/api/admin/events/" + eventId + "/redrive"), HttpMethod.POST,
                new HttpEntity<>(basic("viewer", "viewer123")), String.class);
        assertThat(redrive.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(redrive.getBody()).contains("\"code\":\"FORBIDDEN\"");
    }

    @Test
    void anonymousIsUnauthorizedOnAdminEndpoints() {
        ResponseEntity<String> response = restTemplate.exchange(
                url("/api/admin/events/dead"), HttpMethod.GET,
                new HttpEntity<>(new HttpHeaders()), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getBody()).contains("\"code\":\"UNAUTHORIZED\"");
    }

    @Test
    void adminCanListDeadEvents() {
        createDeadEventId();
        ResponseEntity<String> response = restTemplate.exchange(
                url("/api/admin/events/dead"), HttpMethod.GET,
                new HttpEntity<>(basic("admin", "admin123")), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"DEAD\"").contains("deadReason");
    }

    @Test
    void redriveOnNonDeadEventReturnsConflictWithInvalidState() {
        String eventId = createDeliveredEventId();
        ResponseEntity<String> response = restTemplate.exchange(
                url("/api/admin/events/" + eventId + "/redrive"), HttpMethod.POST,
                new HttpEntity<>(basic("admin", "admin123")), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody()).contains("\"code\":\"INVALID_STATE\"").contains(eventId);
    }

    @Test
    void missingEventReturnsNotFoundWithDistinctCode() {
        ResponseEntity<String> response = restTemplate.exchange(
                url("/api/admin/events/does-not-exist"), HttpMethod.GET,
                new HttpEntity<>(basic("admin", "admin123")), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody())
                .contains("\"code\":\"EVENT_NOT_FOUND\"")
                .contains("does-not-exist");
    }

    @Test
    void businessEndpointRequiresNoRole() {
        HttpHeaders json = new HttpHeaders();
        json.set("Content-Type", "application/json");
        ResponseEntity<String> response = restTemplate.exchange(
                url("/api/orders"), HttpMethod.POST,
                new HttpEntity<>("{ \"customer\": \"\" }", json), String.class);
        // customer 为空 -> BAD_REQUEST（无任何角色要求），而非 401/403
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("\"code\":\"BAD_REQUEST\"");
        assertThat(response.getStatusCode()).isNotEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getStatusCode()).isNotEqualTo(HttpStatus.FORBIDDEN);
    }
}
