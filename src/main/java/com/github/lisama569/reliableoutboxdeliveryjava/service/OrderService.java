package com.github.lisama569.reliableoutboxdeliveryjava.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.EventType;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;
import com.github.lisama569.reliableoutboxdeliveryjava.domain.PurchaseOrder;
import com.github.lisama569.reliableoutboxdeliveryjava.repo.JdbcOrderRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class OrderService {

    public static final String AGGREGATE_TYPE_ORDER = "PurchaseOrder";

    private final JdbcOrderRepository orderRepository;
    private final ObjectMapper objectMapper;

    public OrderService(JdbcOrderRepository orderRepository, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
    }

    public PurchaseOrder createOrder(String orderNo, long amountCents) {
        PurchaseOrder existing = orderRepository.findByOrderNo(orderNo);
        if (existing != null) {
            throw new ConflictException("order already exists: " + orderNo);
        }
        PurchaseOrder order = new PurchaseOrder();
        order.setOrderNo(orderNo);
        order.setAmountCents(amountCents);
        order.setStatus("CREATED");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("orderNo", orderNo);
        payload.put("amountCents", amountCents);
        OutboxEvent event = newEvent(EventType.ORDER_CREATED, orderNo, payload);

        try {
            orderRepository.createWithEvent(order, event);
        } catch (DuplicateKeyException e) {
            throw new ConflictException("order already exists: " + orderNo);
        }
        return orderRepository.findByOrderNo(orderNo);
    }

    public PurchaseOrder payOrder(String orderNo) {
        PurchaseOrder order = orderRepository.findByOrderNo(orderNo);
        if (order == null) {
            throw new NotFoundException("order not found: " + orderNo);
        }
        if ("PAID".equals(order.getStatus())) {
            throw new ConflictException("order already paid: " + orderNo);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("orderNo", orderNo);
        payload.put("paidAt", java.time.Instant.now().toString());
        OutboxEvent event = newEvent(EventType.ORDER_PAID, orderNo, payload);

        orderRepository.markPaidWithEvent(orderNo, event);
        return orderRepository.findByOrderNo(orderNo);
    }

    public PurchaseOrder getOrder(String orderNo) {
        PurchaseOrder order = orderRepository.findByOrderNo(orderNo);
        if (order == null) {
            throw new NotFoundException("order not found: " + orderNo);
        }
        return order;
    }

    private OutboxEvent newEvent(EventType type, String orderNo, Map<String, Object> payload) {
        OutboxEvent event = new OutboxEvent();
        event.setEventId(type.name() + "-" + orderNo + "-"
                + UUID.randomUUID().toString().substring(0, 8));
        event.setAggregateType(AGGREGATE_TYPE_ORDER);
        event.setAggregateId(orderNo);
        event.setEventType(type);
        event.setPayload(toJson(payload));
        return event;
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize event payload", e);
        }
    }
}
