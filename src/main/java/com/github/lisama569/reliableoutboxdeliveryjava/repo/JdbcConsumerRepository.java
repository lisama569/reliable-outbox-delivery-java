package com.github.lisama569.reliableoutboxdeliveryjava.repo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * 消费端幂等存储：以 event_id 作为去重主键。
 * 副作用（更新读模型 order_projection）与“已消费记录”在同一事务提交，
 * 重复投递时 consumeIfFirstTime 返回 false，业务副作用不会执行第二次。
 */
@Repository
public class JdbcConsumerRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcConsumerRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public JdbcConsumerRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 幂等地消费事件：第一次消费时写入去重记录并更新读模型，重复事件直接跳过。
     *
     * @return true 表示第一次消费、副作用已执行；false 表示重复消息、副作用未执行。
     */
    @Transactional
    public boolean consumeIfFirstTime(String eventId, String orderNo, String eventType,
                                      String status) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO consumed_event (event_id, order_no, event_type, consumed_at) "
                            + "VALUES (?, ?, ?, ?)",
                    eventId, orderNo, eventType,
                    OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        } catch (DuplicateKeyException alreadyConsumed) {
            log.info("[consumer] eventId={} duplicate delivery detected, skip side effect",
                    eventId);
            return false;
        }
        jdbcTemplate.update("""
                        MERGE INTO order_projection (order_no, status, event_id, updated_at)
                        KEY (order_no)
                        VALUES (?, ?, ?, ?)
                        """,
                orderNo, status, eventId,
                OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        log.info("[consumer] eventId={} side effect applied once (type={}, orderNo={})",
                eventId, eventType, orderNo);
        return true;
    }

    public int projectionCount() {
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT COUNT(*) AS c FROM order_projection");
        return ((Number) row.get("c")).intValue();
    }

    public int consumedCount() {
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT COUNT(*) AS c FROM consumed_event");
        return ((Number) row.get("c")).intValue();
    }

    public boolean isConsumed(String eventId) {
        Integer found = jdbcTemplate.queryForObject(
                "SELECT 1 FROM consumed_event WHERE event_id = ?", Integer.class, eventId);
        return found != null;
    }
}
