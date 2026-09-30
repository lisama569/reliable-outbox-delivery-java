package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

import com.github.lisama569.reliableoutboxdeliveryjava.config.OutboxProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class BackoffPolicy {

    private final OutboxProperties properties;

    public BackoffPolicy(OutboxProperties properties) {
        this.properties = properties;
    }

    public Duration nextDelay(int failedAttempts) {
        return Duration.ZERO;
    }
}
