package com.github.lisama569.reliableoutboxdeliveryjava.web;

import com.github.lisama569.reliableoutboxdeliveryjava.outbox.EventNotFoundException;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.InvalidEventStateException;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxRepository;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 查询与运维入口。所有端点都要求登录，写操作要求 OPS_ADMIN 角色（见 SecurityConfig）。
 */
@RestController
@RequestMapping("/api/admin/events")
public class AdminController {

    private final OutboxRepository repository;

    public AdminController(OutboxRepository repository) {
        this.repository = repository;
    }

    /** 最近事件列表。 */
    @GetMapping
    public List<Dtos.EventView> recent(@RequestParam(defaultValue = "50") int limit) {
        return repository.findRecent(Math.min(Math.max(limit, 1), 500)).stream()
                .map(AdminController::toView).toList();
    }

    /** 查询单个事件；不存在返回 EVENT_NOT_FOUND。 */
    @GetMapping("/{eventId}")
    public Dtos.EventView getOne(@PathVariable String eventId) {
        return toView(load(eventId));
    }

    /** 某业务聚合下的事件（验证顺序）。 */
    @GetMapping("/aggregate/{aggregateType}/{aggregateId}")
    public List<Dtos.EventView> byAggregate(@PathVariable String aggregateType,
                                            @PathVariable String aggregateId) {
        return repository.findByAggregate(aggregateType, aggregateId).stream()
                .map(AdminController::toView).toList();
    }

    /** 死信列表，含失败原因与尝试次数。 */
    @GetMapping("/dead")
    public List<Dtos.EventView> dead() {
        return repository.findDead().stream().map(AdminController::toView).toList();
    }

    /** 人工重投死信：仅 DEAD 可重投，重投后回到同一套幂等与重试规则。 */
    @PostMapping("/{eventId}/redrive")
    public Dtos.RedriveResponse redrive(@PathVariable String eventId) {
        OutboxEvent event = requireDead(eventId, "redrive");
        boolean ok = repository.redriveDead(eventId);
        if (!ok) {
            throw new InvalidEventStateException(eventId, event.getStatus(), "redrive");
        }
        OutboxEvent refreshed = load(eventId);
        return new Dtos.RedriveResponse(eventId, refreshed.getStatus().name(),
                refreshed.getRedeliveryCount());
    }

    /** 人工标记死信已处理（旁路处理后调用，终态 RESOLVED）。 */
    @PostMapping("/{eventId}/resolve")
    public Dtos.RedriveResponse resolve(@PathVariable String eventId) {
        requireDead(eventId, "resolve");
        boolean ok = repository.resolveDead(eventId);
        if (!ok) {
            throw new InvalidEventStateException(eventId, OutboxStatus.DEAD, "resolve");
        }
        OutboxEvent refreshed = load(eventId);
        return new Dtos.RedriveResponse(eventId, refreshed.getStatus().name(),
                refreshed.getRedeliveryCount());
    }

    private OutboxEvent load(String eventId) {
        return repository.findById(eventId)
                .orElseThrow(() -> new EventNotFoundException(eventId));
    }

    private OutboxEvent requireDead(String eventId, String operation) {
        OutboxEvent event = load(eventId);
        if (event.getStatus() != OutboxStatus.DEAD) {
            throw new InvalidEventStateException(eventId, event.getStatus(), operation);
        }
        return event;
    }

    static Dtos.EventView toView(OutboxEvent e) {
        return new Dtos.EventView(
                e.getId(),
                e.getAggregateType(),
                e.getAggregateId(),
                e.getEventType(),
                e.getStatus().name(),
                e.getAttemptCount(),
                e.getMaxAttempts(),
                e.getRedeliveryCount(),
                e.getNextRetryAt(),
                e.getDeadReason(),
                e.getLastError(),
                e.getLockedBy(),
                e.getCreatedAt());
    }
}
