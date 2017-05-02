package com.bandwidth.sqs.queue;

import static javax.swing.UIManager.put;

import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.QueueAttributeName;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@Immutable
public abstract class SqsQueueAttributeChanges {
    /**
     * 0 - 12 hours, in 1 second increments
     *
     * Default: 30 seconds
     */
    public abstract Optional<Duration> getVisibilityTimeout();

    /**
     * 1 - 256KB, in BYTES
     *
     * Default: 262144 (256 KB)
     */
    public abstract Optional<Integer> getMaxMessageBytes();

    /**
     * 0 - 15 minutes, in 1 second increments
     *
     * Default: 0
     */
    public abstract Optional<Duration> getDeliveryDelay();

    /**
     * The amount of time SQS will retain a message if it does not get deleted
     * Must be between 1 minute and 14 days
     *
     * Default: 4 days
     */
    public abstract Optional<Duration> getMessageRetentionPeriod();

    /**
     * The re-drive policy of the queue. If a message is retried more than the max receive count,
     * it will be transferred automatically to the dead-letter queue
     *
     * Default: No re-drive policy
     */
    public abstract Optional<RedrivePolicy> getRedrivePolicy();

    @Derived
    public Map<String, String> getStringMap() {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder();
        getDeliveryDelay().ifPresent(value -> {
            builder.put(QueueAttributeName.DelaySeconds.toString(), Long.toString(value.getSeconds()));
        });
        getVisibilityTimeout().ifPresent(value -> {
            builder.put(QueueAttributeName.VisibilityTimeout.toString(), Long.toString(value.getSeconds()));
        });
        getMaxMessageBytes().ifPresent(value -> {
            builder.put(QueueAttributeName.MaximumMessageSize.toString(), Integer.toString(value));
        });
        getMessageRetentionPeriod().ifPresent(value -> {
            builder.put(QueueAttributeName.MessageRetentionPeriod.toString(), Long.toString(value.getSeconds()));
        });
        getRedrivePolicy().ifPresent((value -> {
            builder.put(QueueAttributeName.RedrivePolicy.toString(), value.toAttributeString());
        }));
        return builder.build();
    }

    public static ImmutableSqsQueueAttributeChanges.Builder builder() {
        return ImmutableSqsQueueAttributeChanges.builder();
    }
}
