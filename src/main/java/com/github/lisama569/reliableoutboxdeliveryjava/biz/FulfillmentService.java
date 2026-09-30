package com.github.lisama569.reliableoutboxdeliveryjava.biz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * 消费侧业务副作用：收到 OrderCreated 后执行"履约"。
 * processed_event 的幂等插入与副作用写入在同一个事务中完成，
 * 事件标识冲突时整体回滚，副作用绝不会执行第二次。
 */
@Service
public class FulfillmentService {

    private static final Logger log = LoggerFactory.getLogger(FulfillmentService.class);
    public static final String CONSUMER_NAME = "fulfillment-consumer";

    private final JdbcTemplate jdbc;

    public FulfillmentService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return true 表示本次真正执行了副作用；false 表示重复事件被幂等跳过。
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean fulfillOrderIfFirstTime(String eventId, String orderId, String customer) {
        try {
            jdbc.update("""
                    INSERT INTO processed_event (event_id, consumed_at, consumer)
                    VALUES (?, ?, ?)
                    """, eventId, Timestamp.from(Instant.now()), CONSUMER_NAME);
        } catch (DuplicateKeyException alreadyProcessed) {
            log.info("eventId={} duplicate delivery detected, side effect skipped (idempotent)",
                    eventId);
            return false;
        }
        // 幂等标记之后才执行副作用，两者同一事务：失败一起回滚，下轮可重试
        jdbc.update("""
                INSERT INTO fulfillment (order_id, event_id, fulfilled_at)
                VALUES (?, ?, ?)
                """, orderId, eventId, Timestamp.from(Instant.now()));
        log.info("eventId={} side effect executed: order {} fulfilled for customer {}",
                eventId, orderId, customer);
        return true;
    }

    /**
     * 无额外副作用的事件（如发货），也需要按事件标识登记，保证重投幂等。
     *
     * @return true 表示首次处理；false 表示重复事件被跳过。
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markProcessedIfFirstTime(String eventId) {
        try {
            jdbc.update("""
                    INSERT INTO processed_event (event_id, consumed_at, consumer)
                    VALUES (?, ?, ?)
                    """, eventId, Timestamp.from(Instant.now()), CONSUMER_NAME);
        } catch (DuplicateKeyException alreadyProcessed) {
            log.info("eventId={} duplicate delivery detected, skipped (idempotent)", eventId);
            return false;
        }
        return true;
    }
}
