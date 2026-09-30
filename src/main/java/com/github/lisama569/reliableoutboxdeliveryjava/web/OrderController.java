package com.github.lisama569.reliableoutboxdeliveryjava.web;

import com.github.lisama569.reliableoutboxdeliveryjava.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Secured("ROLE_OPS")
    public OrderView create(@Valid @RequestBody CreateOrderRequest request) {
        return OrderView.from(orderService.createOrder(
                request.getOrderNo(), request.getAmountCents()));
    }

    @PostMapping("/payments")
    @ResponseStatus(HttpStatus.OK)
    @Secured("ROLE_OPS")
    public OrderView pay(@Valid @RequestBody PayOrderRequest request) {
        return OrderView.from(orderService.payOrder(request.getOrderNo()));
    }

    @GetMapping("/{orderNo}")
    @Secured({ "ROLE_OPS", "ROLE_VIEWER" })
    public OrderView get(@PathVariable String orderNo) {
        return OrderView.from(orderService.getOrder(orderNo));
    }
}
