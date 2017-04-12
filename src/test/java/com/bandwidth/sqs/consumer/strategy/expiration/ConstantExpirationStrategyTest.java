package com.bandwidth.sqs.consumer.strategy.expiration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.amazonaws.services.sqs.model.Message;
import com.bandwidth.sqs.consumer.TimedMessage;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

public class ConstantExpirationStrategyTest {
    private static final Duration MAX_AGE = Duration.ofMinutes(4);
    private static final Duration NEW_MESSAGE_AGE = Duration.ofMinutes(3);
    private static final Duration EXPIRED_AGE = Duration.ofMinutes(5);

    private final Message messageMock = mock(Message.class);
    private final ConstantExpirationStrategy constantExpirationStrategy = new ConstantExpirationStrategy(MAX_AGE);

    @Test
    public void testIsExpiredTrue() {
        TimedMessage timedMessage = TimedMessage.builder()
                .withMessage(messageMock)
                .withReceivedTime(Instant.now().minus(EXPIRED_AGE))
                .build();
        assertThat(constantExpirationStrategy.isExpired(timedMessage)).isTrue();
    }

    @Test
    public void testIsExpiredFalse() {
        TimedMessage timedMessage = TimedMessage.builder()
                .withMessage(messageMock)
                .withReceivedTime(Instant.now().minus(NEW_MESSAGE_AGE))
                .build();
        assertThat(constantExpirationStrategy.isExpired(timedMessage)).isFalse();
    }

}
