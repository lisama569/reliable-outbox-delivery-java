package com.github.lisama569.reliableoutboxdeliveryjava.web;

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

    @GetMapping
    public List<Object> list(@RequestParam(required = false) String status,
                             @RequestParam(defaultValue = "50") int limit) {
        return List.of();
    }

    @GetMapping("/{id}")
    public Object get(@PathVariable long id) {
        return null;
    }

    @GetMapping("/dead-letters")
    public List<Object> deadLetters(@RequestParam(defaultValue = "50") int limit) {
        return List.of();
    }

    @PostMapping("/{id}/requeue")
    public Object requeue(@PathVariable long id) {
        return null;
    }

    @PostMapping("/{id}/resolve")
    public Object resolve(@PathVariable long id) {
        return null;
    }
}
