package com.github.lisama569.reliableoutboxdeliveryjava.queue;

import java.util.List;

/**
 * 本地模拟消息队列的抽象，语义上按真正的消息系统对待：
 * publish 可能成功、可重试失败或不可重试失败；
 * 消费返回非成功时以至少一次方式重投；消费端需自行按 eventId 幂等。
 */
public interface MessageQueue {

    /**
     * 向队列发布一条消息（带事件标识，用于消费端幂等）。
     */
    PublishOutcome publish(String queueName, String eventId, byte[] payload);

    /**
     * 订阅队列。消费失败返回 RETRYABLE_FAILURE 会被延迟重投；
     * NON_RETRYABLE_FAILURE 进入队列自带的死信队列。
     */
    void subscribe(String queueName, MessageHandler handler);

    /**
     * 让队列把已投递过的消息再投递一次（模拟 broker 重投，验证消费端幂等）。
     * 返回是否找到对应消息并安排了重投。
     */
    boolean resend(String queueName, String eventId);

    /** 队列自带死信队列中的事件标识。 */
    List<String> queueDeadLetterIds(String queueName);

    /** 测试辅助：清空所有内存队列状态（不影响订阅关系）。 */
    void clearAll();

    /** 测试辅助：等待队列中的消息被处理完毕。 */
    boolean awaitQuiet(String queueName, long timeoutMillis) throws InterruptedException;
}
