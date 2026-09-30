package com.github.lisama569.reliableoutboxdeliveryjava.queue;

/**
 * 消费端处理器。返回值即本次消费判定：
 * SUCCESS 表示 ack；RETRYABLE_FAILURE 表示 nack 后由队列延迟重投；
 * NON_RETRYABLE_FAILURE 表示拒绝，进入队列自带的死信队列。
 */
@FunctionalInterface
public interface MessageHandler {

    DeliveryResult handle(Message message) throws Exception;
}
