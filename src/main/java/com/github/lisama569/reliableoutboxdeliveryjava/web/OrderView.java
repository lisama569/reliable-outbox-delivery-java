package com.github.lisama569.reliableoutboxdeliveryjava.web;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.PurchaseOrder;

import java.time.Instant;

public class OrderView {

    private String orderNo;
    private String status;
    private long amountCents;
    private Instant createdAt;
    private Instant updatedAt;

    public static OrderView from(PurchaseOrder o) {
        OrderView v = new OrderView();
        v.orderNo = o.getOrderNo();
        v.status = o.getStatus();
        v.amountCents = o.getAmountCents();
        v.createdAt = o.getCreatedAt();
        v.updatedAt = o.getUpdatedAt();
        return v;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public String getStatus() {
        return status;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
