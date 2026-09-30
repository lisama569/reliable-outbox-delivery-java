package com.github.lisama569.reliableoutboxdeliveryjava.service;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.PurchaseOrder;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOrderRepository;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final JdbcOrderRepository orderRepository;

    public OrderService(JdbcOrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public PurchaseOrder createOrder(String orderNo, long amountCents) {
        return null;
    }

    public PurchaseOrder payOrder(String orderNo) {
        return null;
    }

    public PurchaseOrder getOrder(String orderNo) {
        return null;
    }
}
