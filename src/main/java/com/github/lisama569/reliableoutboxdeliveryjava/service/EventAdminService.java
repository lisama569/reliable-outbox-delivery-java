package com.github.lisama569.reliableoutboxdeliveryjava.service;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.EventStatus;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class EventAdminService {

    private static final Logger log = LoggerFactory.getLogger(EventAdminService.class);

    private final JdbcOutboxRepository repository;

    public EventAdminService(JdbcOutboxRepository repository) {
        this.repository = repository;
    }

    public List<OutboxEvent> listEvents(String status, int limit) {
        return repository.listByStatus(status, limit);
    }

    public OutboxEvent getEvent(long id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotFoundException("event not found: id=" + id));
    }

    public List<OutboxEvent> listDeadLetters(int limit) {
        return repository.listByStatus(EventStatus.DEAD.name(), limit);
    }

    /**
     * 人工重投死信：只有 DEAD 状态可以重投。重投后回到 PENDING 且 attempts 清零，
     * 后续仍走同一套领取 / 退避重试 / 幂等消费规则。
     */
    public OutboxEvent requeueDeadLetter(long id) {
        OutboxEvent event = repository.findById(id)
                .orElseThrow(() -> new NotFoundException("event not found: id=" + id));
        if (event.getStatus() != EventStatus.DEAD) {
            throw new ConflictException("event id=" + id + " is in status "
                    + event.getStatus() + ", only DEAD events can be requeued");
        }
        if (!repository.requeue(id, Instant.now())) {
            throw new ConflictException(
                    "event id=" + id + " state changed concurrently, requeue rejected");
        }
        log.warn("[admin] eventId={} id={} manually requeued from dead-letter",
                event.getEventId(), id);
        return repository.findById(id).orElseThrow();
    }

    /**
     * 人工标记为已处理：只有 DEAD 状态可以流转到 RESOLVED（终态，不再投递）。
     */
    public OutboxEvent resolveDeadLetter(long id) {
        OutboxEvent event = repository.findById(id)
                .orElseThrow(() -> new NotFoundException("event not found: id=" + id));
        if (event.getStatus() != EventStatus.DEAD) {
            throw new ConflictException("event id=" + id + " is in status "
                    + event.getStatus() + ", only DEAD events can be resolved");
        }
        if (!repository.resolve(id)) {
            throw new ConflictException(
                    "event id=" + id + " state changed concurrently, resolve rejected");
        }
        log.warn("[admin] eventId={} id={} manually marked RESOLVED",
                event.getEventId(), id);
        return repository.findById(id).orElseThrow();
    }
}
