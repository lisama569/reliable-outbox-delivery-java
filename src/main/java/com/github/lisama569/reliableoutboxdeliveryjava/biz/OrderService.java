package com.github.lisama569.reliableoutboxdeliveryjava.biz;

import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

/**
 * 示例业务服务：订单状态变更。每次业务写库都与 outbox 事件写入在同一事务中，
 * 因此要么都提交（事件不丢），要么都回滚。
 */
@Service
public class OrderService {

    public static final String AGGREGATE_TYPE = "ORDER";
    public static final String EVENT_ORDER_CREATED = "OrderCreated";
    public static final String EVENT_ORDER_SHIPPED = "OrderShipped";
    public static final String ORDER_CREATED = "CREATED";
    public static final String ORDER_SHIPPED = "SHIPPED";

    private final JdbcTemplate jdbc;
    private final OutboxPublisher outboxPublisher;

    public OrderService(JdbcTemplate jdbc, OutboxPublisher outboxPublisher) {
        this.jdbc = jdbc;
        this.outboxPublisher = outboxPublisher;
    }

    /** 订单创建事件载荷。 */
    public record OrderCreatedPayload(String eventType, String orderId, String customer,
                                      String status) {
    }

    /** 订单发货事件载荷。 */
    public record OrderShippedPayload(String eventType, String orderId, String status) {
    }

    /** 订单创建结果：业务订单 id 与原子落库的事件 id。 */
    public record CreateOrderResult(String orderId, String eventId) {
    }

    @Transactional
    public CreateOrderResult createOrder(String customer) {
        String orderId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        jdbc.update("""
                INSERT INTO biz_order (id, status, customer, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """, orderId, ORDER_CREATED, customer, Timestamp.from(now), Timestamp.from(now));

        // 与业务插入同一事务：提交后事件一定可被投递
        String eventId = outboxPublisher.publish(AGGREGATE_TYPE, orderId, EVENT_ORDER_CREATED,
                new OrderCreatedPayload(EVENT_ORDER_CREATED, orderId, customer, ORDER_CREATED));
        return new CreateOrderResult(orderId, eventId);
    }

    @Transactional
    public String shipOrder(String orderId) {
        Instant now = Instant.now();
        int updated = jdbc.update("""
                UPDATE biz_order SET status = ?, updated_at = ? WHERE id = ?
                """, ORDER_SHIPPED, Timestamp.from(now), orderId);
        if (updated == 0) {
            throw new IllegalArgumentException("order not found: " + orderId);
        }
        return outboxPublisher.publish(AGGREGATE_TYPE, orderId, EVENT_ORDER_SHIPPED,
                new OrderShippedPayload(EVENT_ORDER_SHIPPED, orderId, ORDER_SHIPPED));
    }

    public int countFulfillments() {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM fulfillment", Integer.class);
        return count == null ? 0 : count;
    }
}
