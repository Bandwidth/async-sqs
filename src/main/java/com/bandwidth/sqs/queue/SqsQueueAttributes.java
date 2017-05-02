package com.bandwidth.sqs.queue;

import com.amazonaws.services.sqs.model.QueueAttributeName;

import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Map;

@Immutable
public abstract class SqsQueueAttributes {
    /**
     * 0 - 12 hours, in 1 second increments
     */
    public abstract Duration getVisibilityTimeout();

    /**
     * 1 - 256KB, in BYTES
     */
    public abstract int getMaxMessageBytes();

    /**
     * 0 - 15 minutes, in 1 second increments
     */
    public abstract Duration getDeliveryDelay();

    /**
     * The amount of time SQS will ratain a message if it does not get deleted
     * Must be between 1 minute and 14 days
     */
    public abstract Duration getMessageRetentionPeriod();

    /**
     * The Amazon Resource Name for this queue
     */
    public abstract String getQueueArn();

    public static abstract class Builder {
        public ImmutableSqsQueueAttributes.Builder fromStringMap(Map<String, String> map) {
            long delaySeconds = Long.parseLong(map.get(QueueAttributeName.DelaySeconds.toString()));
            long visibilitySeconds = Long.parseLong(map.get(QueueAttributeName.VisibilityTimeout.toString()));
            int messageBytes = Integer.parseInt(map.get(QueueAttributeName.MaximumMessageSize.toString()));
            long messageRetentionSeconds =
                    Long.parseLong(map.get(QueueAttributeName.MessageRetentionPeriod.toString()));
            String queueArn = map.get(QueueAttributeName.QueueArn.toString());

            return builder()
                    .deliveryDelay(Duration.ofSeconds(delaySeconds))
                    .visibilityTimeout(Duration.ofSeconds(visibilitySeconds))
                    .maxMessageBytes(messageBytes)
                    .messageRetentionPeriod(Duration.ofSeconds(messageRetentionSeconds))
                    .queueArn(queueArn);
        }
    }

    public static ImmutableSqsQueueAttributes.Builder builder() {
        return ImmutableSqsQueueAttributes.builder();
    }
}
