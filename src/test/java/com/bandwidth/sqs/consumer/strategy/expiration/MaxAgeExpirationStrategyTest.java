package com.bandwidth.sqs.consumer.strategy.expiration;

import static org.assertj.core.api.Assertions.assertThat;

import com.bandwidth.sqs.queue.SqsMessage;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

public class MaxAgeExpirationStrategyTest {
    private static final String MESSAGE_BODY = "message body";
    private static final String RECEIPT_HANDLE = "a794lhaef";
    private static final String MESSAGE_ID = "message-id";
    private static final Duration MAX_AGE = Duration.ofMinutes(4);
    private static final Duration NEW_MESSAGE_AGE = Duration.ofMinutes(3);
    private static final Duration EXPIRED_AGE = Duration.ofMinutes(5);

    private final MaxAgeExpirationStrategy maxAgeExpirationStrategy = new MaxAgeExpirationStrategy(MAX_AGE);

    @Test
    public void testIsExpiredTrue() {
        SqsMessage<String> message = SqsMessage.<String>builder()
                .body(MESSAGE_BODY)
                .id(MESSAGE_ID)
                .receivedTime(Instant.now().minus(EXPIRED_AGE))
                .receiptHandle(RECEIPT_HANDLE)
                .build();

        assertThat(maxAgeExpirationStrategy.isExpired(message, null)).isTrue();
    }

    @Test
    public void testIsExpiredFalse() {
        SqsMessage<String> timedMessage = SqsMessage.<String>builder()
                .body(MESSAGE_BODY)
                .id(MESSAGE_ID)
                .receivedTime(Instant.now().minus(NEW_MESSAGE_AGE))
                .receiptHandle(RECEIPT_HANDLE)
                .build();
        assertThat(maxAgeExpirationStrategy.isExpired(timedMessage, null)).isFalse();
    }
}
