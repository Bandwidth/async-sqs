package com.bandwidth.sqs.queue;

import static com.bandwidth.sqs.queue.SqsQueueAttributeTest.ATTRIBUTE_STRING_MAP;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.time.Duration;
import java.util.Map;

public class SqsQueueAttributeChangesTest {
    private static final String REDRIVE_POLICY_STRING =
            "{\"maxReceiveCount\":\"42\",\"deadLetterTargetArn\":\"arn:sqs:123-account-number-456:queue-name\"}";

    public static final Map<String, String> ATTRIBUTE_STRING_MAP = ImmutableMap.<String, String>builder()
            .put("DelaySeconds", "1")
            .put("MaximumMessageSize", "512")
            .put("MessageRetentionPeriod", "345600")
            .put("VisibilityTimeout", "300")
            .put("RedrivePolicy", REDRIVE_POLICY_STRING)
            .put("QueueArn", "arn")
            .build();

    public static final SqsQueueAttributes ATTRIBUTES = SqsQueueAttributes.builder()
            .visibilityTimeout(Duration.ofMinutes(5))
            .messageRetentionPeriod(Duration.ofDays(4))
            .deliveryDelay(Duration.ofSeconds(1))
            .maxMessageBytes(512)
            .queueArn("arn")
            .build();

    @Test
    public void testFromStringMap() {
        SqsQueueAttributes attributes = SqsQueueAttributes.builder().fromStringMap(ATTRIBUTE_STRING_MAP).build();
        assertThat(attributes).isEqualTo(ATTRIBUTES);
    }
}
