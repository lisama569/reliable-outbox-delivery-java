package com.github.lisama569.reliableoutboxdeliveryjava.queue;

/**
 * 单次投递尝试的判定结果。
 */
public enum DeliveryResult {
    /** 投递成功。 */
    SUCCESS,
    /** 可重试失败：按退避策略在尝试上限内重试。 */
    RETRYABLE_FAILURE,
    /** 不可重试失败：直接进入死信。 */
    NON_RETRYABLE_FAILURE
}
