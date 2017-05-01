package com.bandwidth.sqs.queue;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;


@Immutable
public abstract class SqsMessage<T> {
    public abstract String getReceiptHandle();

    public abstract T getBody();

    public abstract String getId();

    @Default
    public Instant getReceivedTime() {
        return Instant.now();
    }

    @Auxiliary
    @JsonIgnore
    public Duration getMessageAge() {
        return Duration.between(getReceivedTime(), Clock.systemUTC().instant());
    }

    public static <T> ImmutableSqsMessage.Builder<T> builder() {
        return ImmutableSqsMessage.builder();
    }
}
