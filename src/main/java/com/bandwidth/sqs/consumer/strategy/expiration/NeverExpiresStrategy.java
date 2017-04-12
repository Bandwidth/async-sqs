package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.consumer.TimedMessage;

/**
 * An expiration strategy that NEVER expires messages
 */
public class NeverExpiresStrategy implements ExpirationStrategy {
    @Override
    public boolean isExpired(TimedMessage timedMessage) {
        return false;
    }
}
