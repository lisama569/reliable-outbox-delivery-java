package com.github.lisama569.reliableoutboxdeliveryjava.repo;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.PurchaseOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * 订单写模型。订单状态变更与 outbox 事件写入在同一个本地事务中提交：
 * 事务提交成功后事件必然可查（不会丢）；事务回滚则两者都不生效。
 */
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
        Instant now = Instant.now();
        jdbcTemplate.update("""
                        INSERT INTO purchase_order (order_no, status, amount_cents, created_at, updated_at)
                        VALUES (?, 'CREATED', ?, ?, ?)
                        """,
                order.getOrderNo(), order.getAmountCents(),
                OffsetDateTime.ofInstant(now, ZoneOffset.UTC),
                OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
        outboxRepository.insert(event);
    }

    @Transactional
    public void markPaidWithEvent(String orderNo, OutboxEvent event) {
        Instant now = Instant.now();
        int updated = jdbcTemplate.update("""
                        UPDATE purchase_order
                           SET status = 'PAID', updated_at = ?
                         WHERE order_no = ? AND status = 'CREATED'
                        """,
                OffsetDateTime.ofInstant(now, ZoneOffset.UTC), orderNo);
        if (updated == 0) {
            throw new IllegalStateException("order not found or already paid: " + orderNo);
        }
        outboxRepository.insert(event);
    }

    public PurchaseOrder findByOrderNo(String orderNo) {
        List<PurchaseOrder> list = jdbcTemplate.query(
                "SELECT * FROM purchase_order WHERE order_no = ?", orderRowMapper(), orderNo);
        return list.stream().findFirst().orElse(null);
    }

    private RowMapper<PurchaseOrder> orderRowMapper() {
        return (rs, rowNum) -> {
            PurchaseOrder o = new PurchaseOrder();
            o.setId(rs.getLong("id"));
            o.setOrderNo(rs.getString("order_no"));
            o.setStatus(rs.getString("status"));
            o.setAmountCents(rs.getLong("amount_cents"));
            o.setCreatedAt(rs.getObject("created_at", OffsetDateTime.class).toInstant());
            o.setUpdatedAt(rs.getObject("updated_at", OffsetDateTime.class).toInstant());
            return o;
        };
    }
}
