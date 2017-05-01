package com.bandwidth.sqs.queue;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

@Immutable
public abstract class SqsQueueClientConfig {
    public static final Duration DEFAULT_BUFFER_DELAY_TIME = Duration.ofMillis(50);

    @Default
    public Duration getBufferDelay() {
        return DEFAULT_BUFFER_DELAY_TIME;
    }

    public static ImmutableSqsQueueClientConfig.Builder builder() {
        return ImmutableSqsQueueClientConfig.builder();
    }
}
