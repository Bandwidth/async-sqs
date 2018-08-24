package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;

import java.time.Duration;

/**
 * Considers a message expired after its age is a certain percentage of the visibility timeout
 */
public class VisibilityTimeoutPercentageExpiration implements ExpirationStrategy {

    private final double percent;

    public VisibilityTimeoutPercentageExpiration(double percent) {
        this.percent = percent;
    }

    @Override
    public boolean isExpired(SqsMessage<?> sqsMessage, Duration visibilityTimeout) {
        long expirationTimeoutMillis = (long) (visibilityTimeout.toMillis() * percent);
        return sqsMessage.getMessageAge().toMillis() > expirationTimeoutMillis;
    }
}
