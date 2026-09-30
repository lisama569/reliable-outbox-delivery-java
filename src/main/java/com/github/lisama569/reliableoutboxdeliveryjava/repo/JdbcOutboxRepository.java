package com.github.lisama569.reliableoutboxdeliveryjava.repo;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcOutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcOutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(OutboxEvent event) {
        // skeleton
    }

    public Optional<OutboxEvent> findById(long id) {
        return Optional.empty();
    }

    public Optional<OutboxEvent> findByEventId(String eventId) {
        return Optional.empty();
    }

    public List<OutboxEvent> listByStatus(String status, int limit) {
        return List.of();
    }

    public Optional<OutboxEvent> claimDue(String workerId, Instant now, int batchSize) {
        return Optional.empty();
    }

    public boolean markDelivered(long id, String workerId) {
        return false;
    }

    public boolean scheduleRetry(long id, String workerId, int attempts,
                                 Instant nextAttemptAt, String error) {
        return false;
    }

    public boolean markDead(long id, String workerId, int attempts,
                            String lastError, String deadReason, Instant now) {
        return false;
    }

    public boolean requeue(long id, Instant now) {
        return false;
    }

    public boolean resolve(long id) {
        return false;
    }

    public int reclaimStale(Instant staleBefore, Instant now) {
        return 0;
    }

    public long countInFlightForAggregate(String aggregateType, String aggregateId) {
        return 0;
    }
}
