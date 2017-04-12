package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.consumer.TimedMessage;

import java.time.Duration;

/**
 * An expiration strategy where the expiration time does not change
 */
public class ConstantExpirationStrategy implements ExpirationStrategy {

    private final Duration maxAge;

    public ConstantExpirationStrategy(Duration maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    public boolean isExpired(TimedMessage timedMessage) {
        Duration timeUntilExpiration = maxAge.minus(timedMessage.getMessageAge());
        return timeUntilExpiration.isNegative();
    }
}
