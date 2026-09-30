package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

import com.github.lisama569.reliableoutboxdeliveryjava.domain.OutboxEvent;

public interface DispatchInterceptors {

    default void beforeMarkDelivered(OutboxEvent event) {
    }
}
