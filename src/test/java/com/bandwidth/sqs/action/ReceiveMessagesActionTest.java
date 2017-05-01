package com.bandwidth.sqs.action;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import org.junit.Test;

import java.time.Duration;
import java.util.Optional;

public class ReceiveMessagesActionTest {

    private static final Optional<Duration> ZERO_DURATION = Optional.of(Duration.ZERO);
    private static final int MAX_MESSAGES = 10;
    private static final String QUEUE_URL = "http://domain.com/path";

    @Test
    public void testCreateRequest() {
        ReceiveMessageRequest request =
                ReceiveMessagesAction.createRequest(QUEUE_URL, MAX_MESSAGES, ZERO_DURATION, ZERO_DURATION);
        assertThat(request.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(request.getMaxNumberOfMessages()).isEqualTo(MAX_MESSAGES);
        assertThat(request.getWaitTimeSeconds()).isEqualTo(0);
        assertThat(request.getVisibilityTimeout()).isEqualTo(0);
    }

    @Test
    public void testConstructor() {
        assertThat(new ReceiveMessagesAction(QUEUE_URL, MAX_MESSAGES, ZERO_DURATION, ZERO_DURATION)).isNotNull();
    }
}
