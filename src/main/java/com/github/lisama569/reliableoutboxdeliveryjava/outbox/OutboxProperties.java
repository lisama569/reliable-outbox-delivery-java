package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * outbox 投递相关配置。
 *
 * @param dispatchers   并发运行的投递器数量（多个投递器竞争同一张表）
 * @param pollBatchSize 每个投递器一次轮询最多领取的事件数
 * @param pollIntervalMillis 投递器轮询间隔
 * @param defaultMaxAttempts 单轮投递的最大尝试次数，超限进入死信
 * @param backoffBaseMillis  指数退避基准（base * 2^(attempt-1)）
 * @param backoffMaxMillis   退避上限
 * @param lockTimeoutMillis  领取锁超时；超时的 IN_PROGRESS 视为进程崩溃残留
 * @param recoveryIntervalMillis 崩溃残留回收扫描间隔
 */
@ConfigurationProperties(prefix = "outbox")
public record OutboxProperties(
        int dispatchers,
        int pollBatchSize,
        long pollIntervalMillis,
        int defaultMaxAttempts,
        long backoffBaseMillis,
        long backoffMaxMillis,
        long lockTimeoutMillis,
        long recoveryIntervalMillis
) {
    public OutboxProperties {
        if (dispatchers <= 0) {
            dispatchers = 2;
        }
        if (pollBatchSize <= 0) {
            pollBatchSize = 16;
        }
        if (pollIntervalMillis <= 0) {
            pollIntervalMillis = 50;
        }
        if (defaultMaxAttempts <= 0) {
            defaultMaxAttempts = 5;
        }
        if (backoffBaseMillis <= 0) {
            backoffBaseMillis = 100;
        }
        if (backoffMaxMillis <= 0) {
            backoffMaxMillis = 5000;
        }
        if (lockTimeoutMillis <= 0) {
            lockTimeoutMillis = 30_000;
        }
        if (recoveryIntervalMillis <= 0) {
            recoveryIntervalMillis = 1000;
        }
    }

    /** 计算第 attempt 次失败后的退避时长（attempt 从 1 开始）。 */
    public long backoffMillisFor(int attempt) {
        double raw = backoffBaseMillis * Math.pow(2, attempt - 1);
        return Math.min(backoffMaxMillis, (long) raw);
    }
}
