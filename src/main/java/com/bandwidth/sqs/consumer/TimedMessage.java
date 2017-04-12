package com.bandwidth.sqs.consumer;

import com.amazonaws.services.sqs.model.Message;
import com.catapult.messaging.model.CatapultImmutable;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import com.bandwidth.sqs.consumer.ImmutableTimedMessage.Builder;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * An SQS message that keeps track of the time it was received
 */

@Immutable
@CatapultImmutable
public abstract class TimedMessage {

    public abstract Message getMessage();

    @Default
    public Instant getReceivedTime() {
        return Instant.now();
    }

    @Auxiliary
    @JsonIgnore
    public Duration getMessageAge() {
        return Duration.between(getReceivedTime(), Clock.systemUTC().instant());
    }

    public static Builder builder() {
        return ImmutableTimedMessage.builder();
    }
}
