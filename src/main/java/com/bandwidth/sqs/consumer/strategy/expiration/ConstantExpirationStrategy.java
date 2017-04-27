package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;

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
    public boolean isExpired(SqsMessage<String> timedMessage) {
        Duration timeUntilExpiration = maxAge.minus(timedMessage.getMessageAge());
        return timeUntilExpiration.isNegative();
    }
}
