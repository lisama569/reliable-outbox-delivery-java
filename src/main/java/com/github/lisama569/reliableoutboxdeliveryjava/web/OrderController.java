package com.github.lisama569.reliableoutboxdeliveryjava.web;

import com.github.lisama569.reliableoutboxdeliveryjava.service.OrderService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public Object create(@RequestBody CreateOrderRequest request) {
        return null;
    }

    @PostMapping("/payments")
    public Object pay(@RequestBody PayOrderRequest request) {
        return null;
    }
}
