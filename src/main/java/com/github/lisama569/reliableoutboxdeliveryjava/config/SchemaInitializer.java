package com.github.lisama569.reliableoutboxdeliveryjava.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 启动时创建表结构。使用 H2 文件库时 CREATE TABLE IF NOT EXISTS 可重复执行，
 * 因此“进程重启”后数据与表结构都会保留。
 */
@Component
@Order(0)
public class SchemaInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);

    private static final String CREATE_ORDERS = """
            CREATE TABLE IF NOT EXISTS purchase_order (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                order_no VARCHAR(64) NOT NULL UNIQUE,
                status VARCHAR(32) NOT NULL,
                amount_cents BIGINT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
            """;

    private static final String CREATE_OUTBOX = """
            CREATE TABLE IF NOT EXISTS outbox_event (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                event_id VARCHAR(64) NOT NULL UNIQUE,
                aggregate_type VARCHAR(64) NOT NULL,
                aggregate_id VARCHAR(64) NOT NULL,
                event_type VARCHAR(32) NOT NULL,
                payload CLOB NOT NULL,
                status VARCHAR(16) NOT NULL,
                attempts INT NOT NULL DEFAULT 0,
                next_attempt_at TIMESTAMP WITH TIME ZONE,
                locked_by VARCHAR(128),
                locked_at TIMESTAMP WITH TIME ZONE,
                last_error VARCHAR(2000),
                dead_reason VARCHAR(2000),
                dead_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
            """;

    private static final String CREATE_OUTBOX_INDEXES = """
            CREATE INDEX IF NOT EXISTS idx_outbox_dispatch
                ON outbox_event (status, next_attempt_at)
            """;

    private static final String CREATE_OUTBOX_AGG_INDEX = """
            CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
                ON outbox_event (aggregate_type, aggregate_id, id)
            """;

    private static final String CREATE_CONSUMED = """
            CREATE TABLE IF NOT EXISTS consumed_event (
                event_id VARCHAR(64) PRIMARY KEY,
                order_no VARCHAR(64),
                event_type VARCHAR(32) NOT NULL,
                consumed_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
            """;

    private static final String CREATE_PROJECTION = """
            CREATE TABLE IF NOT EXISTS order_projection (
                order_no VARCHAR(64) PRIMARY KEY,
                status VARCHAR(32) NOT NULL,
                event_id VARCHAR(64) NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
            """;

    private final JdbcTemplate jdbcTemplate;

    public SchemaInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        jdbcTemplate.execute(CREATE_ORDERS);
        jdbcTemplate.execute(CREATE_OUTBOX);
        jdbcTemplate.execute(CREATE_OUTBOX_INDEXES);
        jdbcTemplate.execute(CREATE_OUTBOX_AGG_INDEX);
        jdbcTemplate.execute(CREATE_CONSUMED);
        jdbcTemplate.execute(CREATE_PROJECTION);
        log.info("Schema initialized (purchase_order/outbox_event/consumed_event/order_projection)");
    }
}
