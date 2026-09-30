package com.github.lisama569.reliableoutboxdeliveryjava.service;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOutboxRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EventAdminService {

    private final JdbcOutboxRepository repository;

    public EventAdminService(JdbcOutboxRepository repository) {
        this.repository = repository;
    }

    public List<OutboxEvent> listEvents(String status, int limit) {
        return List.of();
    }

    public OutboxEvent getEvent(long id) {
        return null;
    }

    public List<OutboxEvent> listDeadLetters(int limit) {
        return List.of();
    }

    public OutboxEvent requeueDeadLetter(long id) {
        return null;
    }

    public OutboxEvent resolveDeadLetter(long id) {
        return null;
    }
}
