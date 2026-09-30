package com.github.lisama569.reliableoutboxdeliveryjava.web;

import jakarta.validation.constraints.NotBlank;

public class PayOrderRequest {

    @NotBlank
    private String orderNo;

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }
}
