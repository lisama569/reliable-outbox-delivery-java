package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.Application;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.EventStatus;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcConsumerRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.tests.support.TestFailureControl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 场景五：进程重启。
 * 1) 第一个进程：事件被消费、副作用已落库，但标记 DELIVERED 前投递线程被“强杀”；
 * 2) 关闭整个 Spring 上下文（= 进程退出，内存队列随之消失）；
 * 3) 第二个进程用同一个文件 H2 重启，孤儿回收把事件重新投递；
 * 4) 事件最终 DELIVERED，副作用仍只有一次。
 */
class ProcessRestartTest {

    @TempDir
    Path tempDir;

    @Test
    void eventSurvivesFullProcessRestartWithoutDuplicateSideEffect() throws Exception {
        String unique = Long.toHexString(System.nanoTime());
        String orderNo = "ORD-RESTART-" + unique;
        String dbFile = tempDir.resolve("restartdb-" + unique).toString();
        String jdbcUrl = "jdbc:h2:file:" + dbFile.replace('\\', '/');

        // ---- 第一个进程：制造“消费成功后、确认落库前崩溃” ----
        try (ConfigurableApplicationContext first = startContext(jdbcUrl)) {
            TestFailureControl.FailureState failureState =
                    first.getBean(TestFailureControl.FailureState.class);
            failureState.crashBeforeMarkDelivered.set(1);

            Integer port = first.getEnvironment()
                    .getProperty("local.server.port", Integer.class);
            HttpClient client = HttpClient.newHttpClient();
            String body = "{\"orderNo\":\"" + orderNo + "\",\"amountCents\":42}";
            HttpRequest httpReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/api/orders"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Basic " + Base64.getEncoder()
                            .encodeToString("ops:ops-pass".getBytes()))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = client.send(httpReq,
                    HttpResponse.BodyHandlers.ofString());
            assertThat(resp.statusCode()).isEqualTo(201);

            JdbcOutboxRepository outbox = first.getBean(JdbcOutboxRepository.class);
            OutboxEvent event = await().atMost(Duration.ofSeconds(10)).until(
                    () -> outbox.listByStatus(null, 10).stream().findFirst(),
                    java.util.Optional::isPresent).get();

            // 等待 worker 线程被强杀、事件停留在 PROCESSING（副作用已执行）
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                OutboxEvent stuck = outbox.findByEventId(event.getEventId()).orElseThrow();
                assertThat(stuck.getStatus()).isEqualTo(EventStatus.PROCESSING);
            });
            JdbcConsumerRepository consumer1 = first.getBean(JdbcConsumerRepository.class);
            assertThat(consumer1.projectionCount()).isEqualTo(1);
            assertThat(consumer1.consumedCount()).isEqualTo(1);

            // ---- 进程退出：关闭整个上下文，内存队列丢弃 ----
        }

        // ---- 第二个进程：使用同一个文件库重启，不注入任何故障 ----
        try (ConfigurableApplicationContext second = startContext(jdbcUrl)) {
            JdbcOutboxRepository outbox2 = second.getBean(JdbcOutboxRepository.class);
            JdbcConsumerRepository consumer2 = second.getBean(JdbcConsumerRepository.class);

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                List<OutboxEvent> delivered = outbox2.listByStatus(
                        EventStatus.DELIVERED.name(), 10);
                assertThat(delivered).hasSize(1);
                assertThat(delivered.get(0).getAttempts()).isGreaterThanOrEqualTo(2);
            });

            // 关键断言：经历重启与重新投递，业务副作用仍只执行一次
            assertThat(consumer2.projectionCount()).isEqualTo(1);
            assertThat(consumer2.consumedCount()).isEqualTo(1);
        }
    }

    @org.springframework.context.annotation.Configuration
    static class TestDataSourceConfig {

        static String url;

        @Bean
        @Primary
        public DataSource dataSource() {
            DriverManagerDataSource ds = new DriverManagerDataSource();
            ds.setDriverClassName("org.h2.Driver");
            ds.setUrl(url);
            ds.setUsername("sa");
            ds.setPassword("");
            return ds;
        }
    }

    private ConfigurableApplicationContext startContext(String jdbcUrl) {
        TestDataSourceConfig.url = jdbcUrl;
        return new SpringApplicationBuilder(Application.class, TestFailureControl.class,
                        TestDataSourceConfig.class)
                .web(WebApplicationType.SERVLET)
                .properties(
                        "server.port=0",
                        "outbox.poll-interval=30ms",
                        "outbox.reclaim-interval=200ms",
                        "outbox.visibility-timeout=10s",
                        "outbox.backoff-initial=50ms",
                        "logging.level.com.github.lisama569=INFO")
                .run();
    }
}
