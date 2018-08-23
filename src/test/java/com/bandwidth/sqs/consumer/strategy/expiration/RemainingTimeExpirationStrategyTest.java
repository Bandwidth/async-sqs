package com.bandwidth.sqs.consumer.strategy.expiration;

import com.bandwidth.sqs.queue.SqsMessage;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class RemainingTimeExpirationStrategyTest {
    private static final String MESSAGE_BODY = "message body";
    private static final String RECEIPT_HANDLE = "a794lhaef";
    private static final String MESSAGE_ID = "message-id";
    private static final Duration MESSAGE_AGE = Duration.ofMinutes(1);
    private static final Duration REQUIRED_TIME = Duration.ofSeconds(30);
    private static final Duration EXPIRED_VISIBILITY_TIMEOUT = Duration.ofSeconds(31);
    private static final Duration NOT_EXPIRED_VISIBILITY_TIMEOUT = Duration.ofMinutes(2);

    private final RemainingTimeExpirationStrategy maxAgeExpirationStrategy =
            new RemainingTimeExpirationStrategy(REQUIRED_TIME);


    @Test
    public void testIsExpiredTrue() {
        SqsMessage<String> message = SqsMessage.<String>builder()
                .body(MESSAGE_BODY)
                .id(MESSAGE_ID)
                .receivedTime(Instant.now().minus(MESSAGE_AGE))
                .receiptHandle(RECEIPT_HANDLE)
                .build();

        assertThat(maxAgeExpirationStrategy.isExpired(message, EXPIRED_VISIBILITY_TIMEOUT)).isTrue();
    }

    @Test
    public void testIsExpiredFalse() {
        SqsMessage<String> timedMessage = SqsMessage.<String>builder()
                .body(MESSAGE_BODY)
                .id(MESSAGE_ID)
                .receivedTime(Instant.now().minus(MESSAGE_AGE))
                .receiptHandle(RECEIPT_HANDLE)
                .build();
        assertThat(maxAgeExpirationStrategy.isExpired(timedMessage, NOT_EXPIRED_VISIBILITY_TIMEOUT)).isFalse();
    }
}
