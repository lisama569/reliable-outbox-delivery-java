package com.github.lisama569.reliableoutboxdeliveryjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    private int dispatcherInstances = 2;
    private Duration pollInterval = Duration.ofMillis(200);
    private int maxAttempts = 5;
    private Duration backoffInitial = Duration.ofMillis(100);
    private double backoffMultiplier = 2.0;
    private Duration backoffMax = Duration.ofSeconds(5);
    private Duration visibilityTimeout = Duration.ofSeconds(2);
    private Duration reclaimInterval = Duration.ofSeconds(1);
    private int claimBatchSize = 10;
    private boolean enabled = true;

    public int getDispatcherInstances() {
        return dispatcherInstances;
    }

    public void setDispatcherInstances(int dispatcherInstances) {
        this.dispatcherInstances = dispatcherInstances;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getBackoffInitial() {
        return backoffInitial;
    }

    public void setBackoffInitial(Duration backoffInitial) {
        this.backoffInitial = backoffInitial;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public Duration getBackoffMax() {
        return backoffMax;
    }

    public void setBackoffMax(Duration backoffMax) {
        this.backoffMax = backoffMax;
    }

    public Duration getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(Duration visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public Duration getReclaimInterval() {
        return reclaimInterval;
    }

    public void setReclaimInterval(Duration reclaimInterval) {
        this.reclaimInterval = reclaimInterval;
    }

    public int getClaimBatchSize() {
        return claimBatchSize;
    }

    public void setClaimBatchSize(int claimBatchSize) {
        this.claimBatchSize = claimBatchSize;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
