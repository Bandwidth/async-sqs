package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;

/**
 * An expiration strategy that NEVER expires messages
 */
public class NeverExpiresStrategy implements ExpirationStrategy {
    @Override
    public boolean isExpired(SqsMessage<?> timedMessage) {
        return false;
    }
}
