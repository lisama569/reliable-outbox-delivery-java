package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

import com.github.lisama569.reliableoutboxdeliveryjava.config.OutboxProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 指数退避：initial * multiplier^(failedAttempts-1)，封顶为 backoffMax。
 * failedAttempts 表示已经失败的次数（1,2,3...）。
 */
@Component
public class BackoffPolicy {

    private final OutboxProperties properties;

    public BackoffPolicy(OutboxProperties properties) {
        this.properties = properties;
    }

    public Duration nextDelay(int failedAttempts) {
        if (failedAttempts < 1) {
            return Duration.ZERO;
        }
        double factor = Math.pow(properties.getBackoffMultiplier(), failedAttempts - 1);
        long millis = Math.round(properties.getBackoffInitial().toMillis() * factor);
        long capped = Math.min(millis, properties.getBackoffMax().toMillis());
        return Duration.ofMillis(capped);
    }
}
