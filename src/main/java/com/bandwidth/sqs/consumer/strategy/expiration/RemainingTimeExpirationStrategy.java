package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;

import java.time.Duration;

public class RemainingTimeExpirationStrategy implements ExpirationStrategy {
    private final Duration minRemainingTime;

    public RemainingTimeExpirationStrategy(Duration minRemainingTime) {
        this.minRemainingTime = minRemainingTime;
    }

    @Override
    public boolean isExpired(SqsMessage<?> sqsMessage, Duration visibilityTimeout) {
        Duration remainingTime = visibilityTimeout.minus(sqsMessage.getMessageAge());
        return remainingTime.compareTo(minRemainingTime) < 0;
    }
}
