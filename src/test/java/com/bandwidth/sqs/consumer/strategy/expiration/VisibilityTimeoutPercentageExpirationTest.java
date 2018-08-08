package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class VisibilityTimeoutPercentageExpirationTest {
    private static final String MESSAGE_BODY = "message body";
    private static final String RECEIPT_HANDLE = "a794lhaef";
    private static final String MESSAGE_ID = "message-id";
    private static final Duration EXPIRED_AGE = Duration.ofMinutes(3);
    private static final Duration VISIBILITY_TIMEOUT = Duration.ofMinutes(5);

    private final VisibilityTimeoutPercentageExpiration strategy = new VisibilityTimeoutPercentageExpiration(0.5);

    @Test
    public void testIsExpiredFalse() {
        SqsMessage<String> message = SqsMessage.<String>builder()
                .body(MESSAGE_BODY)
                .id(MESSAGE_ID)
                .receivedTime(Instant.now())
                .receiptHandle(RECEIPT_HANDLE)
                .build();

        assertThat(strategy.isExpired(message, VISIBILITY_TIMEOUT)).isFalse();
    }

    @Test
    public void testIsExpiredTrue() {
        SqsMessage<String> message = SqsMessage.<String>builder()
                .body(MESSAGE_BODY)
                .id(MESSAGE_ID)
                .receivedTime(Instant.now().minus(EXPIRED_AGE))
                .receiptHandle(RECEIPT_HANDLE)
                .build();

        assertThat(strategy.isExpired(message, VISIBILITY_TIMEOUT)).isTrue();
    }
}
