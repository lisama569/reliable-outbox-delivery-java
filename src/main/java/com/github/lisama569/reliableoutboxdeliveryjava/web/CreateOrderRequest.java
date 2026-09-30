package com.github.lisama569.reliableoutboxdeliveryjava.web;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class CreateOrderRequest {

    @NotBlank
    private String orderNo;

    @Min(1)
    private long amountCents;

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public void setAmountCents(long amountCents) {
        this.amountCents = amountCents;
    }
}
