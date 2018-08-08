package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;

import java.time.Duration;

/**
 * An expiration strategy that NEVER expires messages
 */
public class NeverExpiresStrategy implements ExpirationStrategy {
    @Override
    public boolean isExpired(SqsMessage<?> timedMessage, Duration visibilityTimeout) {
        return false;
    }
}
