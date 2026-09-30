package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

/**
 * 事件生命周期状态：
 *
 * <pre>
 *   PENDING ──领取──▶ IN_PROGRESS ──成功──▶ DELIVERED
 *                        │  可重试失败且未超限
 *                        ├────重试────▶ IN_PROGRESS（下次轮询）
 *                        │  可重试失败超限 / 不可重试失败
 *                        └───────────▶ DEAD
 *   DEAD ──人工重投──▶ PENDING；DEAD ──人工标记已处理──▶ RESOLVED
 *   （进程崩溃后残留的 IN_PROGRESS 由回收扫描重置为 PENDING）
 * </pre>
 */
public enum OutboxStatus {
    /** 待投递（新建或死信重投后）。 */
    PENDING,
    /** 已被某个投递器领取，正在投递。 */
    IN_PROGRESS,
    /** 已成功送达消息队列。 */
    DELIVERED,
    /** 投递失败进入死信，等待人工介入。 */
    DEAD,
    /** 死信被人工标记为已处理（终态，不再投递）。 */
    RESOLVED
}
