package com.bandwidth.sqs.consumer.strategy.backoff;

import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;

import java.time.Duration;

/**
 * Calculates delay time for exponential backoff.
 * The formula used is (percentFailures)^exponent * maxDelayMillis = delayTimeMillis
 * <p>
 * This gives you a smooth curve up to the maxDelayMillis. You can choose the exponent
 * to determine how "steep" the curve is.
 */
public class ExponentialBackoffStrategy implements BackoffStrategy {
    private final Duration windowSize;
    private final Duration maxDelay;
    private final double exponent;

    public ExponentialBackoffStrategy(Duration windowSize, Duration maxDelay, double exponent) {
        this.windowSize = windowSize;
        this.maxDelay = maxDelay;
        this.exponent = exponent;
    }

    @Override
    public Duration getWindowSize() {
        return windowSize;
    }

    @Override
    public Duration getDelayTime(double percentFailures) {
        return Duration.ofMillis((int) (Math.pow(percentFailures, exponent) * maxDelay.toMillis()));
    }
}
