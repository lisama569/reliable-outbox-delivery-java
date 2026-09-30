package com.github.lisama569.reliableoutboxdeliveryjava.queue;

/**
 * 发布结果：成功 / 可重试失败 / 不可重试失败，并携带失败原因。
 */
public record PublishOutcome(DeliveryResult result, String error) {

    public static PublishOutcome success() {
        return new PublishOutcome(DeliveryResult.SUCCESS, null);
    }

    public static PublishOutcome retryable(String error) {
        return new PublishOutcome(DeliveryResult.RETRYABLE_FAILURE, error);
    }

    public static PublishOutcome nonRetryable(String error) {
        return new PublishOutcome(DeliveryResult.NON_RETRYABLE_FAILURE, error);
    }

    public boolean succeeded() {
        return result == DeliveryResult.SUCCESS;
    }
}
