package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;

import java.time.Duration;

public interface ExpirationStrategy {
    boolean isExpired(SqsMessage<?> timedMessage, Duration visibilityTimeout);
}
