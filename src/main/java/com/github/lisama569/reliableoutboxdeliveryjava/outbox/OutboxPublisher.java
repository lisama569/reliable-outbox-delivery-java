package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

import com.github.lisama569.reliableoutboxdeliveryjava.support.JsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * 业务代码通过本组件在"业务变更所在的同一个事务"里写入 outbox 事件，
 * 事务提交后事件即已持久化，进程崩溃也不会丢失。
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository repository;
    private final OutboxProperties properties;
    private final JsonSupport jsonSupport;

    public OutboxPublisher(OutboxRepository repository, OutboxProperties properties,
                           JsonSupport jsonSupport) {
        this.repository = repository;
        this.properties = properties;
        this.jsonSupport = jsonSupport;
    }

    /**
     * 在当前事务内插入一条 PENDING 事件。
     */
    public String publish(String aggregateType, String aggregateId, String eventType,
                          Object payload) {
        OutboxEvent event = buildEvent(aggregateType, aggregateId, eventType, payload);
        repository.insert(event);
        log.info("outbox event stored eventId={} aggregate={}:{} type={}",
                event.getId(), aggregateType, aggregateId, eventType);
        return event.getId();
    }

    public OutboxEvent buildEvent(String aggregateType, String aggregateId, String eventType,
                                  Object payload) {
        OutboxEvent event = new OutboxEvent();
        event.setId(UUID.randomUUID().toString());
        event.setAggregateType(aggregateType);
        event.setAggregateId(aggregateId);
        event.setEventType(eventType);
        event.setPayload(jsonSupport.toJson(payload));
        event.setStatus(OutboxStatus.PENDING);
        event.setAttemptCount(0);
        event.setMaxAttempts(properties.defaultMaxAttempts());
        event.setNextRetryAt(null);
        event.setRedeliveryCount(0);
        event.setCreatedAt(Instant.now());
        return event;
    }
}
