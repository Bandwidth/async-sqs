package com.bandwidth.sqs.queue;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.time.Duration;
import java.util.Map;

public class SqsQueueAttributeTest {
    private static final Duration RETENTION_PERIOD = Duration.ofDays(4);
    private static final int MAX_MESSAGE_BYTES = 512;
    private static final Duration DELIVERY_DELAY = Duration.ofSeconds(1);
    private static final Duration VISIBILITY_TIMEOUT = Duration.ofMinutes(5);
    private static final String REDRIVE_POLICY_STRING =
            "{\"maxReceiveCount\":\"42\",\"deadLetterTargetArn\":\"arn:sqs:123-account-number-456:queue-name\"}";
    private static final RedrivePolicy REDRIVE_POLICY = RedrivePolicy.builder()
            .deadLetterTargetArn("arn:sqs:123-account-number-456:queue-name")
            .maxReceiveCount(42)
            .build();
    static final Map<String, String> ATTRIBUTE_STRING_MAP = ImmutableMap.<String, String>builder()
            .put("DelaySeconds", "1")
            .put("MaximumMessageSize", "512")
            .put("MessageRetentionPeriod", "345600")
            .put("VisibilityTimeout", "300")
            .put("RedrivePolicy", REDRIVE_POLICY_STRING)
            .build();

    @Test
    public void testGetStringMap() {
        MutableSqsQueueAttributes attributes = MutableSqsQueueAttributes.builder()
                .messageRetentionPeriod(RETENTION_PERIOD)
                .maxMessageBytes(MAX_MESSAGE_BYTES)
                .deliveryDelay(DELIVERY_DELAY)
                .visibilityTimeout(VISIBILITY_TIMEOUT)
                .redrivePolicy(REDRIVE_POLICY)
                .build();
        assertThat(attributes.getStringMap()).isEqualTo(ATTRIBUTE_STRING_MAP);
    }
}
