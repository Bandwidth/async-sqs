package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.consumer.TimedMessage;

public interface ExpirationStrategy {
    boolean isExpired(TimedMessage timedMessage);
}
