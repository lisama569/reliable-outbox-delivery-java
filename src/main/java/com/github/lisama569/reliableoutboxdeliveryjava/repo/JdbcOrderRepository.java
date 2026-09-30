package com.github.lisama569.reliableoutboxdeliveryjava.repo;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.EventType;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.PurchaseOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcOrderRepository {

    private final JdbcTemplate jdbcTemplate;
    private final JdbcOutboxRepository outboxRepository;

    public JdbcOrderRepository(JdbcTemplate jdbcTemplate,
                               JdbcOutboxRepository outboxRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.outboxRepository = outboxRepository;
    }

    @Transactional
    public void createWithEvent(PurchaseOrder order, OutboxEvent event) {
        // skeleton
    }

    @Transactional
    public void markPaidWithEvent(String orderNo, OutboxEvent event) {
        // skeleton
    }

    public PurchaseOrder findByOrderNo(String orderNo) {
        return null;
    }
}
