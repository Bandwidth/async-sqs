package com.bandwidth.sqs.queue;

import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.QueueAttributeName;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@Immutable
public abstract class SqsQueueAttributes {
    /**
     * 0 - 12 hours, in 1 second increments
     *
     * Default: 30 seconds
     */
    @Default
    public Duration getVisibilityTimeout() {
        return Duration.ofSeconds(30);
    }

    /**
     * 1 - 256KB, in BYTES
     *
     * Default: 262144 (256 KB)
     */
    @Default
    public int getMaxMessageBytes() {
        return 256 * 1024;
    }

    /**
     * 0 - 15 minutes, in 1 second increments
     *
     * Default: 0
     */
    @Default
    public Duration getDeliveryDelay() {
        return Duration.ZERO;
    }

    /**
     * The amount of time SQS will ratain a message if it does not get deleted
     * Must be between 1 minute and 14 days
     *
     * Default: 4 days
     */
    @Default
    public Duration getMessageRetentionPeriod() {
        return Duration.ofDays(4);
    }

    @Derived
    public Map<String, String> getStringMap() {
        //TODO: add deadletter config
        return ImmutableMap.<String, String>builder()
                .put(QueueAttributeName.DelaySeconds.toString(),
                        Long.toString(getDeliveryDelay().getSeconds()))
                .put(QueueAttributeName.VisibilityTimeout.toString(),
                        Long.toString(getVisibilityTimeout().getSeconds()))
                .put(QueueAttributeName.MaximumMessageSize.toString(),
                        Integer.toString(getMaxMessageBytes()))
                .put(QueueAttributeName.MessageRetentionPeriod.toString(),
                        Long.toString(getMessageRetentionPeriod().getSeconds()))
                .build();
    }

    public static abstract class Builder {
        public ImmutableSqsQueueAttributes.Builder fromStringMap(Map<String, String> map) {
            long delaySeconds = Long.parseLong(map.get(QueueAttributeName.DelaySeconds.toString()));
            long visibilitySeconds = Long.parseLong(map.get(QueueAttributeName.VisibilityTimeout.toString()));
            int messageBytes = Integer.parseInt(map.get(QueueAttributeName.MaximumMessageSize.toString()));
            long messageRetentionSeconds =
                    Long.parseLong(map.get(QueueAttributeName.MessageRetentionPeriod.toString()));

            return builder()
                    .deliveryDelay(Duration.ofSeconds(delaySeconds))
                    .visibilityTimeout(Duration.ofSeconds(visibilitySeconds))
                    .maxMessageBytes(messageBytes)
                    .messageRetentionPeriod(Duration.ofSeconds(messageRetentionSeconds));
        }
    }

    public static ImmutableSqsQueueAttributes.Builder builder() {
        return ImmutableSqsQueueAttributes.builder();
    }
}
