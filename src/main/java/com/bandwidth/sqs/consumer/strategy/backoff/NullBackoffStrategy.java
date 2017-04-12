package com.bandwidth.sqs.consumer.strategy.backoff;

import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;

import java.time.Duration;

public class NullBackoffStrategy implements BackoffStrategy {
    @Override
    public Duration getWindowSize() {
        return Duration.ZERO;
    }

    @Override
    public Duration getDelayTime(double percentFailures) {
        return Duration.ZERO;
    }
}
