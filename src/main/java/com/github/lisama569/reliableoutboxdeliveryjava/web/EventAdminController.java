package com.github.lisama569.reliableoutboxdeliveryjava.web;

import com.github.lisama569.reliableoutboxdeliveryjava.service.EventAdminService;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/admin/events")
public class EventAdminController {

    private final EventAdminService eventAdminService;

    public EventAdminController(EventAdminService eventAdminService) {
        this.eventAdminService = eventAdminService;
    }

    /**
     * 查看事件列表（可按状态过滤）：OPS 与 VIEWER 均可。
     */
    @GetMapping
    @Secured({ "ROLE_OPS", "ROLE_VIEWER" })
    public List<EventView> list(@RequestParam(required = false) String status,
                                @RequestParam(defaultValue = "50") int limit) {
        return eventAdminService.listEvents(status, limit).stream()
                .map(EventView::from)
                .toList();
    }

    @GetMapping("/{id}")
    @Secured({ "ROLE_OPS", "ROLE_VIEWER" })
    public EventView get(@PathVariable long id) {
        return EventView.from(eventAdminService.getEvent(id));
    }

    @GetMapping("/dead-letters")
    @Secured({ "ROLE_OPS", "ROLE_VIEWER" })
    public List<EventView> deadLetters(@RequestParam(defaultValue = "50") int limit) {
        return eventAdminService.listDeadLetters(limit).stream()
                .map(EventView::from)
                .toList();
    }

    /**
     * 死信重投：仅 OPS。
     */
    @PostMapping("/{id}/requeue")
    @Secured("ROLE_OPS")
    public EventView requeue(@PathVariable long id) {
        return EventView.from(eventAdminService.requeueDeadLetter(id));
    }

    /**
     * 死信标记为已处理：仅 OPS。
     */
    @PostMapping("/{id}/resolve")
    @Secured("ROLE_OPS")
    public EventView resolve(@PathVariable long id) {
        return EventView.from(eventAdminService.resolveDeadLetter(id));
    }
}
