package com.github.lisama569.reliableoutboxdeliveryjava.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcConsumerRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcConsumerRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public boolean consumeIfFirstTime(String eventId, String orderNo, String eventType,
                                      String payload, Runnable sideEffect) {
        return false;
    }

    public int projectionCount() {
        return 0;
    }

    public boolean isConsumed(String eventId) {
        return false;
    }
}
