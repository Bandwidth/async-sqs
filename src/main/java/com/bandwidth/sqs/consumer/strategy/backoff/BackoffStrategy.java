package com.bandwidth.sqs.consumer.strategy.backoff;


import java.time.Duration;

/**
 * A backoff strategy to introduce delay depending on the percent of failures detected within
 * the last {@link #getWindowSize()}` duration
 */
public interface BackoffStrategy {
    /**
     * @return The window of time in milliseconds that request error percentage is calculated
     */
    Duration getWindowSize();

    /**
     * @return The delay for the next message
     */
    Duration getDelayTime(double percentFailures);
}
