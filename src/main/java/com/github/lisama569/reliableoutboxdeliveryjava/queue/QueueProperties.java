package com.github.lisama569.reliableoutboxdeliveryjava.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地模拟消息队列的配置。
 *
 * @param queueName        订单事件队列名
 * @param consumerWorkers  消费工作线程数
 * @param redeliveryDelayMillis 消费 nack 后的重投延迟
 */
@ConfigurationProperties(prefix = "localqueue")
public record QueueProperties(
        String queueName,
        int consumerWorkers,
        long redeliveryDelayMillis
) {
    public QueueProperties {
        if (queueName == null || queueName.isBlank()) {
            queueName = "order.events";
        }
        if (consumerWorkers <= 0) {
            consumerWorkers = 2;
        }
        if (redeliveryDelayMillis < 0) {
            redeliveryDelayMillis = 200;
        }
    }
}
