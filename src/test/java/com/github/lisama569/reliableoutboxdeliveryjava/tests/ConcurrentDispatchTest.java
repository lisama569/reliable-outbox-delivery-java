package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.biz.OrderService;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 并发场景：两个投递器同时运行。
 * 1) 确定性地让两个执行者同时处理不同事件（发布屏障），证明并发领取真实发生；
 * 2) 每条事件的审计行恰好一条，且事件总数 == 审计行数，证明没有重复领取/重复处理。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConcurrentDispatchTest extends AbstractIntegrationTest {

    @Autowired
    private OrderService orderService;
    @Autowired
    private OutboxRepository repository;
    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void manyEventsClaimedExactlyOnceAcrossDispatchers() throws Exception {
        int total = 24;

        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            final int idx = i;
            futures.add(pool.submit(() ->
                    orderService.createOrder("concurrent-" + idx).eventId()));
        }
        List<String> eventIds = new ArrayList<>();
        for (Future<String> f : futures) {
            eventIds.add(f.get());
        }
        pool.shutdown();

        // 恢复投递前，在发布处设两人屏障：两个投递器必须同时到达才放行
        failureSimulator.armPublishBarrier(2);
        resumeDispatchers();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(repository.countByStatus(OutboxStatus.DELIVERED)).isEqualTo(total));
        failureSimulator.disarmPublishBarrier();

        // 无残留锁定/待处理
        assertThat(repository.countByStatus(OutboxStatus.IN_PROGRESS)).isZero();
        assertThat(repository.countByStatus(OutboxStatus.PENDING)).isZero();

        // 每条事件恰好被一个执行者处理一次（审计断言，重复领取会多出行）
        for (String eventId : eventIds) {
            Integer rows = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM dispatch_audit WHERE event_id = ?",
                    Integer.class, eventId);
            assertThat(rows).as("event %s processed exactly once", eventId).isEqualTo(1);
        }
        Integer auditTotal = jdbc.queryForObject(
                "SELECT COUNT(*) FROM dispatch_audit", Integer.class);
        assertThat(auditTotal).isEqualTo(total);

        // 屏障机制本身已要求两个投递器并发到达；再确认两个执行者都留有审计
        List<String> dispatchersUsed = jdbc.queryForList(
                "SELECT DISTINCT dispatcher_id FROM dispatch_audit", String.class);
        assertThat(dispatchersUsed).hasSize(2);

        // 消费端副作用：每个订单恰好一次履约
        assertThat(orderService.countFulfillments()).isEqualTo(total);
    }
}
