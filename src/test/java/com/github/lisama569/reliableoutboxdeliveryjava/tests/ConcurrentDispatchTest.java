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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 场景六：多个投递器并发运行。
 * - 同一条事件不会被两个执行者同时处理（每个事件恰好消费一次，attempts 无异常叠加）；
 * - 同一业务聚合的多个事件严格按 id 顺序处理，队头阻塞不会让后来者越过前面的事件；
 * - 不同聚合可并行，整体吞吐不受单线程限制（4 个投递器并发跑量）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = { com.github.lisama569.reliableoutboxdeliveryjava.Application.class,
                TestFailureControl.class })
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:concurrent;DB_CLOSE_DELAY=-1",
        "outbox.poll-interval=20ms",
        "outbox.reclaim-interval=500ms",
        "outbox.visibility-timeout=10s",
        "outbox.dispatcher-instances=4"
})
class ConcurrentDispatchTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcOutboxRepository outboxRepository;

    @Autowired
    private JdbcConsumerRepository consumerRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void concurrentDispatchersDoNotDoubleClaimAndKeepAggregateOrder() throws Exception {
        HttpHeaders ops = TestAuth.ops();

        // 第一部分：多聚合并发创建 40 个订单，4 个投递器并发投递
        int orderCount = 40;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < orderCount; i++) {
            String orderNo = String.format("ORD-C-%03d", i);
            futures.add(pool.submit(() -> {
                try { start.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw new RuntimeException(ie); }
                HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                        Map.of("orderNo", orderNo, "amountCents", 10L), ops);
                restTemplate.exchange("/api/orders", HttpMethod.POST, req, String.class);
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(outboxRepository.listByStatus(EventStatus.DELIVERED.name(), 200))
                        .hasSize(orderCount));

        // 每条事件恰好消费一次：consumed_event 40 行、投影 40 行、没有事件 attempts>1
        assertThat(consumerRepository.consumedCount()).isEqualTo(orderCount);
        assertThat(consumerRepository.projectionCount()).isEqualTo(orderCount);
        List<OutboxEvent> delivered = outboxRepository.listByStatus(
                EventStatus.DELIVERED.name(), 200);
        assertThat(delivered).allSatisfy(e ->
                assertThat(e.getAttempts()).as("event " + e.getEventId()).isEqualTo(1));
        assertThat(delivered).extracting(OutboxEvent::getLockedBy)
                .filteredOn(v -> v != null)
                .isEmpty();

        // 第二部分：同一聚合的 3 个事件必须按 id 升序投递（CREATED 先于 PAID）
        HttpEntity<Map<String, Object>> createReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-SEQ", "amountCents", 1L), ops);
        assertThat(restTemplate.exchange("/api/orders", HttpMethod.POST, createReq, String.class)
                .getStatusCode().is2xxSuccessful()).isTrue();
        HttpEntity<Map<String, Object>> payReq = new HttpEntity<>(
                Map.of("orderNo", "ORD-SEQ"), ops);
        assertThat(restTemplate.exchange("/api/orders/payments", HttpMethod.POST, payReq,
                String.class).getStatusCode().is2xxSuccessful()).isTrue();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Map<String, Object>> seqEvents = jdbcTemplate.queryForList(
                    "SELECT event_type, status FROM outbox_event "
                            + "WHERE aggregate_id = ? ORDER BY id", "ORD-SEQ");
            assertThat(seqEvents).hasSize(2);
            assertThat(seqEvents.get(0).get("EVENT_TYPE")).isEqualTo("ORDER_CREATED");
            assertThat(seqEvents.get(1).get("EVENT_TYPE")).isEqualTo("ORDER_PAID");
            assertThat(seqEvents).extracting(m -> m.get("STATUS"))
                    .containsOnly("DELIVERED");
        });

        // 读模型最终态是 PAID（若乱序，CREATED 可能覆盖 PAID）
        Map<String, Object> projection = jdbcTemplate.queryForMap(
                "SELECT status FROM order_projection WHERE order_no = ?", "ORD-SEQ");
        assertThat(projection.get("STATUS")).isEqualTo("PAID");
    }
}
