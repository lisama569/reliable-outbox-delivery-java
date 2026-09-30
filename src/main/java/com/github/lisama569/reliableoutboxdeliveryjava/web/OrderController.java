package com.github.lisama569.reliableoutboxdeliveryjava.web;

import com.github.lisama569.reliableoutboxdeliveryjava.biz.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 业务入口：创建订单/发货。无需角色（演示用），写库与事件落库原子完成。
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<Dtos.CreateOrderResponse> create(
            @RequestBody Dtos.CreateOrderRequest request) {
        if (request == null || request.customer() == null || request.customer().isBlank()) {
            throw new IllegalArgumentException("customer must not be blank");
        }
        OrderService.CreateOrderResult result = orderService.createOrder(request.customer());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new Dtos.CreateOrderResponse(result.orderId(), result.eventId()));
    }

    @PostMapping("/{orderId}/ship")
    public Dtos.ShipOrderResponse ship(@PathVariable String orderId) {
        String eventId = orderService.shipOrder(orderId);
        return new Dtos.ShipOrderResponse(orderId, eventId);
    }
}
