package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.biz.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 进程重启场景：使用独立的 H2 文件库，先在第一个"进程"中让事件停留在 IN_PROGRESS，
 * 然后不优雅地关闭（模拟崩溃），第二个"进程"用同一文件库启动，靠锁超时回收把残留事件
 * 重新投递成功，验证提交后的事件不丢、且消费端幂等使副作用只发生一次。
 */
class RestartRecoveryTest {

    private ConfigurableApplicationContext startContext(Path dbDir, String... extra) {
        String url = "jdbc:h2:file:" + dbDir.resolve("restartdb")
                + ";AUTO_SERVER=FALSE;DB_CLOSE_ON_EXIT=FALSE";
        String[] args = new String[extra.length + 2];
        args[0] = "--spring.profiles.active=restart";
        args[1] = "--spring.datasource.url=" + url;
        System.arraycopy(extra, 0, args, 2, extra.length);
        return new SpringApplicationBuilder(
                com.github.lisama569.reliableoutboxdeliveryjava.Application.class)
                .web(WebApplicationType.SERVLET)
                .properties(args)
                .run();
    }

    @Test
    void eventStuckInProgressAfterCrashIsRecoveredByRestartedProcess(@TempDir Path dbDir)
            throws Exception {
        ConfigurableApplicationContext first = startContext(dbDir);
        try {
            // 暂停投递器，制造一条已经落库但尚未投递的事件
            var group = first.getBean(
                    com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxSchedulerConfig.class);
            group.pauseAll();
            var orderService = first.getBean(OrderService.class);
            String eventId = orderService.createOrder("heidi").eventId();
            String orderId = first.getBean(JdbcTemplate.class).queryForObject(
                    "SELECT aggregate_id FROM outbox_event WHERE id = ?",
                    String.class, eventId);

            // 模拟崩溃：直接关闭容器（不等待任何优雅收尾），事件仍是 PENDING
            first.close();
            first = null;

            // 第二个进程用同一文件库启动：事件应被投递并消费，且副作用只执行一次
            ConfigurableApplicationContext second = startContext(dbDir);
            try {
                JdbcTemplate jdbc = second.getBean(JdbcTemplate.class);
                waitForStatus(jdbc, eventId, "DELIVERED", 20_000);
                waitForProcessed(jdbc, eventId, 20_000);

                Integer fulfillments = jdbc.queryForObject(
                        "SELECT COUNT(*) FROM fulfillment WHERE order_id = ?",
                        Integer.class, orderId);
                assertThat(fulfillments).isEqualTo(1);
                Integer processed = jdbc.queryForObject(
                        "SELECT COUNT(*) FROM processed_event WHERE event_id = ?",
                        Integer.class, eventId);
                assertThat(processed).isEqualTo(1);
            } finally {
                second.close();
            }
        } finally {
            if (first != null) {
                first.close();
            }
        }
    }

    @Test
    void inProgressLeaseAfterCrashIsReclaimedOnRestart(@TempDir Path dbDir) throws Exception {
        ConfigurableApplicationContext first = startContext(dbDir);
        String eventId;
        String orderId;
        try {
            var group = first.getBean(
                    com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxSchedulerConfig.class);
            group.pauseAll();
            var orderService = first.getBean(OrderService.class);
            eventId = orderService.createOrder("ivan").eventId();
            JdbcTemplate firstJdbc = first.getBean(JdbcTemplate.class);
            orderId = firstJdbc.queryForObject(
                    "SELECT aggregate_id FROM outbox_event WHERE id = ?",
                    String.class, eventId);
            // 手工把事件置为"上个进程崩溃时停留在 IN_PROGRESS 且锁已超时"
            firstJdbc.update("UPDATE outbox_event SET status='IN_PROGRESS', locked_by='dead-dispatcher',"
                            + " locked_at=TIMESTAMPADD('SECOND', -60, CURRENT_TIMESTAMP) WHERE id=?",
                    eventId);
            first.close();
            first = null;

            ConfigurableApplicationContext second = startContext(dbDir);
            try {
                JdbcTemplate jdbc = second.getBean(JdbcTemplate.class);
                waitForStatus(jdbc, eventId, "DELIVERED", 20_000);
                waitForProcessed(jdbc, eventId, 20_000);
                assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*) FROM fulfillment WHERE order_id = ?",
                        Integer.class, orderId)).isEqualTo(1);
            } finally {
                second.close();
            }
        } finally {
            if (first != null) {
                first.close();
            }
        }
    }

    private void waitForStatus(JdbcTemplate jdbc, String eventId, String status, long timeoutMs)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            Integer n = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM outbox_event WHERE id = ? AND status = ?",
                    Integer.class, eventId, status);
            if (n != null && n == 1) {
                return;
            }
            Thread.sleep(50);
        }
        String current = jdbc.queryForObject(
                "SELECT status FROM outbox_event WHERE id = ?", String.class, eventId);
        throw new AssertionError("event " + eventId + " did not reach " + status
                + ", current=" + current);
    }

    /** 等待消费端按 eventId 幂等落库（副作用与 processed 在同一事务）。 */
    private void waitForProcessed(JdbcTemplate jdbc, String eventId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            Integer n = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM processed_event WHERE event_id = ?",
                    Integer.class, eventId);
            if (n != null && n == 1) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("event " + eventId + " was not consumed within timeout");
    }
}
