package com.github.lisama569.reliableoutboxdeliveryjava.consumer;

/**
 * 消费故障注入钩子（测试用）。实现可以针对某个 eventId 的前若干次投递抛出异常。
 */
public interface ConsumerFailureHook {

    void checkDelivery(String eventId, int deliveryIndex);
}
