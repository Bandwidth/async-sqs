package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;

public interface ExpirationStrategy {
    boolean isExpired(SqsMessage<?> timedMessage);
}
