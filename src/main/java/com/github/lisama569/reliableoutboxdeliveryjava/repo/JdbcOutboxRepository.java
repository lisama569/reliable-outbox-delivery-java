package com.github.lisama569.reliableoutboxdeliveryjava.repo;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.EventStatus;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

/**
 * outbox 事件表的 JDBC 实现。
 *
 * 领取（claimDue）使用单条 UPDATE ... WHERE id = (子查询) 完成：
 * 1) 只挑选 PENDING/RETRY 且到了可投递时间的事件；
 * 2) 同一聚合内存在更早的非终态事件时跳过（队头阻塞，保证同聚合顺序）；
 * 3) 外层条件在写锁上再次校验状态，两个并发执行者即使子查询选中同一行，
 *    也只有一个 UPDATE 能命中，从而保证同一条事件不会被重复领取。
 */
@Repository
public class JdbcOutboxRepository {

    private static final String NON_TERMINAL_STATES =
            "('PENDING','PROCESSING','RETRY','DEAD')";

    private final JdbcTemplate jdbcTemplate;

    public JdbcOutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(OutboxEvent event) {
        Instant now = Instant.now();
        jdbcTemplate.update("""
                        INSERT INTO outbox_event
                          (event_id, aggregate_type, aggregate_id, event_type, payload,
                           status, attempts, next_attempt_at, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, 'PENDING', 0, ?, ?, ?)
                        """,
                event.getEventId(),
                event.getAggregateType(),
                event.getAggregateId(),
                event.getEventType().name(),
                event.getPayload(),
                ts(event.getNextAttemptAt() != null ? event.getNextAttemptAt() : now),
                ts(now),
                ts(now));
    }

    public Optional<OutboxEvent> findById(long id) {
        List<OutboxEvent> list = jdbcTemplate.query(
                "SELECT * FROM outbox_event WHERE id = ?", eventRowMapper(), id);
        return list.stream().findFirst();
    }

    public Optional<OutboxEvent> findByEventId(String eventId) {
        List<OutboxEvent> list = jdbcTemplate.query(
                "SELECT * FROM outbox_event WHERE event_id = ?", eventRowMapper(), eventId);
        return list.stream().findFirst();
    }

    public List<OutboxEvent> listByStatus(String status, int limit) {
        int bounded = Math.min(Math.max(limit, 1), 500);
        if (status == null || status.isBlank()) {
            return jdbcTemplate.query(
                    "SELECT * FROM outbox_event ORDER BY id DESC FETCH FIRST ? ROWS ONLY",
                    eventRowMapper(), bounded);
        }
        EventStatus parsed = EventStatus.valueOf(status.trim().toUpperCase());
        return jdbcTemplate.query(
                "SELECT * FROM outbox_event WHERE status = ? ORDER BY id DESC FETCH FIRST ? ROWS ONLY",
                eventRowMapper(), parsed.name(), bounded);
    }

    /**
     * 原子地领取一条到期事件；没有可领取事件时返回 empty。
     */
    @Transactional
    public Optional<OutboxEvent> claimDue(String workerId, Instant now) {
        int updated = jdbcTemplate.update("""
                        UPDATE outbox_event
                           SET status = 'PROCESSING',
                               locked_by = ?,
                               locked_at = ?,
                               attempts = attempts + 1,
                               updated_at = ?
                         WHERE id = (
                                 SELECT e.id
                                   FROM outbox_event e
                                  WHERE e.status IN ('PENDING','RETRY')
                                    AND (e.next_attempt_at IS NULL OR e.next_attempt_at <= ?)
                                    AND NOT EXISTS (
                                            SELECT 1
                                              FROM outbox_event b
                                             WHERE b.aggregate_type = e.aggregate_type
                                               AND b.aggregate_id = e.aggregate_id
                                               AND b.id < e.id
                                               AND b.status IN ('PENDING','PROCESSING','RETRY','DEAD')
                                        )
                                  ORDER BY e.id
                                  FETCH FIRST 1 ROW ONLY
                               )
                           AND status IN ('PENDING','RETRY')
                        """,
                workerId, ts(now), ts(now), ts(now));
        if (updated == 0) {
            return Optional.empty();
        }
        List<OutboxEvent> claimed = jdbcTemplate.query("""
                        SELECT * FROM outbox_event
                         WHERE status = 'PROCESSING' AND locked_by = ?
                         ORDER BY id DESC FETCH FIRST 1 ROW ONLY
                        """,
                eventRowMapper(), workerId);
        return claimed.stream().findFirst();
    }

    public boolean markDelivered(long id, String workerId) {
        Instant now = Instant.now();
        return jdbcTemplate.update("""
                        UPDATE outbox_event
                           SET status = 'DELIVERED',
                               locked_by = NULL,
                               locked_at = NULL,
                               next_attempt_at = NULL,
                               updated_at = ?
                         WHERE id = ? AND locked_by = ? AND status = 'PROCESSING'
                        """,
                ts(now), id, workerId) == 1;
    }

    public boolean scheduleRetry(long id, String workerId, int attempts,
                                 Instant nextAttemptAt, String error) {
        Instant now = Instant.now();
        return jdbcTemplate.update("""
                        UPDATE outbox_event
                           SET status = 'RETRY',
                               attempts = ?,
                               next_attempt_at = ?,
                               last_error = ?,
                               locked_by = NULL,
                               locked_at = NULL,
                               updated_at = ?
                         WHERE id = ? AND locked_by = ? AND status = 'PROCESSING'
                        """,
                attempts, ts(nextAttemptAt), clip(error), ts(now), id, workerId) == 1;
    }

    public boolean markDead(long id, String workerId, int attempts,
                            String lastError, String deadReason, Instant now) {
        return jdbcTemplate.update("""
                        UPDATE outbox_event
                           SET status = 'DEAD',
                               attempts = ?,
                               last_error = ?,
                               dead_reason = ?,
                               dead_at = ?,
                               next_attempt_at = NULL,
                               locked_by = NULL,
                               locked_at = NULL,
                               updated_at = ?
                         WHERE id = ? AND locked_by = ? AND status = 'PROCESSING'
                        """,
                attempts, clip(lastError), clip(deadReason), ts(now), ts(now),
                id, workerId) == 1;
    }

    /**
     * 死信重投：仅 DEAD 可重投，重置尝试次数，按新事件重新排队。
     */
    public boolean requeue(long id, Instant now) {
        return jdbcTemplate.update("""
                        UPDATE outbox_event
                           SET status = 'PENDING',
                               attempts = 0,
                               next_attempt_at = ?,
                               last_error = NULL,
                               dead_reason = NULL,
                               dead_at = NULL,
                               locked_by = NULL,
                               locked_at = NULL,
                               updated_at = ?
                         WHERE id = ? AND status = 'DEAD'
                        """,
                ts(now), ts(now), id) == 1;
    }

    /**
     * 人工标记为已处理：仅 DEAD 可流转到 RESOLVED（终态）。
     */
    public boolean resolve(long id) {
        Instant now = Instant.now();
        return jdbcTemplate.update("""
                        UPDATE outbox_event
                           SET status = 'RESOLVED',
                               locked_by = NULL,
                               locked_at = NULL,
                               next_attempt_at = NULL,
                               updated_at = ?
                         WHERE id = ? AND status = 'DEAD'
                        """,
                ts(now), id) == 1;
    }

    /**
     * 回收长时间停留在 PROCESSING 的事件（投递器所在进程崩溃/被杀）。
     * 回收后回到 RETRY 且立即到期，由存活的投递器重新投递（至少一次）。
     */
    public int reclaimStale(Instant staleBefore, Instant now) {
        return jdbcTemplate.update("""
                        UPDATE outbox_event
                           SET status = 'RETRY',
                               next_attempt_at = ?,
                               locked_by = NULL,
                               locked_at = NULL,
                               updated_at = ?
                         WHERE status = 'PROCESSING' AND locked_at < ?
                        """,
                ts(now), ts(now), ts(staleBefore));
    }

    private RowMapper<OutboxEvent> eventRowMapper() {
        return (rs, rowNum) -> {
            OutboxEvent e = new OutboxEvent();
            e.setId(rs.getLong("id"));
            e.setEventId(rs.getString("event_id"));
            e.setAggregateType(rs.getString("aggregate_type"));
            e.setAggregateId(rs.getString("aggregate_id"));
            e.setEventType(com.github.lisama569.reliableoutboxdeliveryjava.domain.EventType
                    .valueOf(rs.getString("event_type")));
            e.setPayload(rs.getString("payload"));
            e.setStatus(EventStatus.valueOf(rs.getString("status")));
            e.setAttempts(rs.getInt("attempts"));
            e.setNextAttemptAt(instant(rs.getObject("next_attempt_at", OffsetDateTime.class)));
            e.setLockedBy(rs.getString("locked_by"));
            e.setLockedAt(instant(rs.getObject("locked_at", OffsetDateTime.class)));
            e.setLastError(rs.getString("last_error"));
            e.setDeadReason(rs.getString("dead_reason"));
            e.setDeadAt(instant(rs.getObject("dead_at", OffsetDateTime.class)));
            e.setCreatedAt(instant(rs.getObject("created_at", OffsetDateTime.class)));
            e.setUpdatedAt(instant(rs.getObject("updated_at", OffsetDateTime.class)));
            return e;
        };
    }

    private static OffsetDateTime ts(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant instant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }

    private static String clip(String text) {
        if (text == null) {
            return null;
        }
        return text.length() <= 1900 ? text : text.substring(0, 1900) + "...(truncated)";
    }
}
