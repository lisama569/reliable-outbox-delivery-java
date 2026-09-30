package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * outbox 表数据访问。所有领取/流转操作都使用条件 UPDATE 保证并发安全：
 * 多个投递器同时竞争时，数据库行锁保证同一条事件只有一个领取成功。
 */
@Repository
public class OutboxRepository {

    private static final String COLUMNS = """
            id, aggregate_type, aggregate_id, event_type, payload, status,
            attempt_count, max_attempts, next_retry_at, locked_by, locked_at,
            dead_reason, last_error, redelivery_count, created_at, updated_at
            """;

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<OutboxEvent> MAPPER = (rs, n) -> {
        OutboxEvent e = new OutboxEvent();
        e.setId(rs.getString("id"));
        e.setAggregateType(rs.getString("aggregate_type"));
        e.setAggregateId(rs.getString("aggregate_id"));
        e.setEventType(rs.getString("event_type"));
        e.setPayload(rs.getString("payload"));
        e.setStatus(OutboxStatus.valueOf(rs.getString("status")));
        e.setAttemptCount(rs.getInt("attempt_count"));
        e.setMaxAttempts(rs.getInt("max_attempts"));
        Timestamp nrt = rs.getTimestamp("next_retry_at");
        e.setNextRetryAt(nrt == null ? null : nrt.toInstant());
        e.setLockedBy(rs.getString("locked_by"));
        Timestamp la = rs.getTimestamp("locked_at");
        e.setLockedAt(la == null ? null : la.toInstant());
        e.setDeadReason(rs.getString("dead_reason"));
        e.setLastError(rs.getString("last_error"));
        e.setRedeliveryCount(rs.getInt("redelivery_count"));
        e.setCreatedAt(rs.getTimestamp("created_at").toInstant());
        e.setUpdatedAt(rs.getTimestamp("updated_at").toInstant());
        return e;
    };

    /** 与业务变更在同一事务内调用。 */
    public void insert(OutboxEvent event) {
        Instant now = Instant.now();
        jdbc.update("""
                INSERT INTO outbox_event (id, aggregate_type, aggregate_id, event_type, payload,
                    status, attempt_count, max_attempts, next_retry_at, locked_by, locked_at,
                    dead_reason, last_error, redelivery_count, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                event.getId(), event.getAggregateType(), event.getAggregateId(),
                event.getEventType(), event.getPayload(), event.getStatus().name(),
                event.getAttemptCount(), event.getMaxAttempts(), event.getNextRetryAt(),
                event.getLockedBy(), event.getLockedAt(), event.getDeadReason(),
                event.getLastError(), event.getRedeliveryCount(),
                Timestamp.from(event.getCreatedAt() != null ? event.getCreatedAt() : now),
                Timestamp.from(now));
    }

    /**
     * 原子领取一批可投递事件。在单个事务内先用 {@code SELECT ... FOR UPDATE} 对候选行加行锁，
     * 再用条件 UPDATE 置为 IN_PROGRESS。多个投递器并发时，后到事务的 FOR UPDATE 会阻塞等待，
     * 待前者提交后基于最新数据重新求值（已被领取的行不再满足 PENDING），因此同一条事件不会
     * 被重复领取。
     * WHERE 中的两层保护：
     * 1) 只领取 PENDING 且退避窗口已过的事件；
     * 2) 同一聚合若已有未终态（PENDING/IN_PROGRESS/DEAD）的更早事件，则跳过，
     *    保证同一业务聚合的事件不会并发/乱序投递。
     */
    @org.springframework.transaction.annotation.Transactional
    public List<OutboxEvent> claimBatch(String dispatcherId, int limit) {
        Instant now = Instant.now();
        String selectSql = """
                SELECT id FROM outbox_event e
                WHERE e.status = 'PENDING'
                  AND (e.next_retry_at IS NULL OR e.next_retry_at <= ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM outbox_event p
                      WHERE p.aggregate_type = e.aggregate_type
                        AND p.aggregate_id = e.aggregate_id
                        AND (p.created_at < e.created_at
                             OR (p.created_at = e.created_at AND p.id < e.id))
                        AND p.status IN ('PENDING', 'IN_PROGRESS', 'DEAD')
                  )
                ORDER BY e.created_at, e.id
                FETCH FIRST ? ROWS ONLY
                FOR UPDATE
                """;
        return jdbc.execute((java.sql.Connection con) -> {
            List<String> claimedIds = new java.util.ArrayList<>();
            try (java.sql.PreparedStatement ps = con.prepareStatement(selectSql)) {
                ps.setTimestamp(1, Timestamp.from(now));
                ps.setInt(2, limit);
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        claimedIds.add(rs.getString(1));
                    }
                }
            }
            List<OutboxEvent> result = new java.util.ArrayList<>();
            for (String id : claimedIds) {
                int updated;
                try (java.sql.PreparedStatement up = con.prepareStatement("""
                        UPDATE outbox_event
                        SET status = 'IN_PROGRESS', locked_by = ?, locked_at = ?, updated_at = ?
                        WHERE id = ? AND status = 'PENDING'
                        """)) {
                    up.setString(1, dispatcherId);
                    up.setTimestamp(2, Timestamp.from(now));
                    up.setTimestamp(3, Timestamp.from(now));
                    up.setString(4, id);
                    updated = up.executeUpdate();
                }
                if (updated > 0) {
                    findById(id).ifPresent(result::add);
                }
            }
            return result;
        });
    }

    /** 投递成功：置为 DELIVERED 并清理锁/退避/错误，保留实际尝试次数。 */
    public void markDelivered(String id, int attemptCount) {
        jdbc.update("""
                UPDATE outbox_event
                SET status = 'DELIVERED', locked_by = NULL, locked_at = NULL,
                    next_retry_at = NULL, last_error = NULL, attempt_count = ?, updated_at = ?
                WHERE id = ?
                """, attemptCount, Timestamp.from(Instant.now()), id);
    }

    /** 可重试失败但未超限：回到 PENDING，退避后再被领取。 */
    public void markRetryPending(String id, int attemptCount, Instant nextRetryAt, String error) {
        jdbc.update("""
                UPDATE outbox_event
                SET status = 'PENDING', locked_by = NULL, locked_at = NULL,
                    attempt_count = ?, next_retry_at = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """, attemptCount, Timestamp.from(nextRetryAt), error,
                Timestamp.from(Instant.now()), id);
    }

    /** 进入死信：保留失败原因、尝试次数与锁信息。 */
    public void markDead(String id, int attemptCount, String reason, String error) {
        jdbc.update("""
                UPDATE outbox_event
                SET status = 'DEAD', locked_by = NULL, locked_at = NULL,
                    attempt_count = ?, dead_reason = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """, attemptCount, reason, error, Timestamp.from(Instant.now()), id);
    }

    /** 人工重投死信：只有 DEAD 可流转，尝试计数归零、重投次数加一。 */
    public boolean redriveDead(String id) {
        int updated = jdbc.update("""
                UPDATE outbox_event
                SET status = 'PENDING', attempt_count = 0, next_retry_at = NULL,
                    locked_by = NULL, locked_at = NULL,
                    redelivery_count = redelivery_count + 1, updated_at = ?
                WHERE id = ? AND status = 'DEAD'
                """, Timestamp.from(Instant.now()), id);
        return updated > 0;
    }

    /** 人工标记死信已处理：只有 DEAD 可流转。 */
    public boolean resolveDead(String id) {
        int updated = jdbc.update("""
                UPDATE outbox_event
                SET status = 'RESOLVED', locked_by = NULL, locked_at = NULL,
                    updated_at = ?
                WHERE id = ? AND status = 'DEAD'
                """, Timestamp.from(Instant.now()), id);
        return updated > 0;
    }

    /**
     * 回收进程崩溃后残留的 IN_PROGRESS 锁（超过 lockTimeout）。
     * 聚合顺序保护仍在：重置为 PENDING 后下次领取照常排队。
     */
    public int recoverStaleLocks(String dispatcherId, Instant lockedBefore) {
        return jdbc.update("""
                UPDATE outbox_event
                SET status = 'PENDING', locked_by = NULL, locked_at = NULL, updated_at = ?
                WHERE status = 'IN_PROGRESS' AND locked_at < ?
                """, Timestamp.from(Instant.now()), Timestamp.from(lockedBefore));
    }

    /** 诊断：统计当前满足回收条件的 IN_PROGRESS 行数。 */
    public int countStaleInProgress(Instant lockedBefore) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event "
                        + "WHERE status = 'IN_PROGRESS' AND locked_at < ?",
                Integer.class, Timestamp.from(lockedBefore));
        return n == null ? 0 : n;
    }

    public Optional<OutboxEvent> findById(String id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM outbox_event WHERE id = ?",
                MAPPER, id).stream().findFirst();
    }

    public List<OutboxEvent> findByAggregate(String aggregateType, String aggregateId) {
        return jdbc.query(
                "SELECT " + COLUMNS
                        + " FROM outbox_event WHERE aggregate_type = ? AND aggregate_id = ?"
                        + " ORDER BY created_at, id",
                MAPPER, aggregateType, aggregateId);
    }

    public List<OutboxEvent> findDead() {
        return jdbc.query(
                "SELECT " + COLUMNS + " FROM outbox_event WHERE status = 'DEAD'"
                        + " ORDER BY updated_at",
                MAPPER);
    }

    public List<OutboxEvent> findRecent(int limit) {
        return jdbc.query(
                "SELECT " + COLUMNS + " FROM outbox_event ORDER BY created_at DESC, id"
                        + " FETCH FIRST ? ROWS ONLY",
                MAPPER, limit);
    }

    public int countByStatus(OutboxStatus status) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE status = ?", Integer.class, status.name());
        return count == null ? 0 : count;
    }
}
