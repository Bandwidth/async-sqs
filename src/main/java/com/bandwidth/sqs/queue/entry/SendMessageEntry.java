package com.bandwidth.sqs.queue.entry;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.subjects.SingleSubject;

@Immutable
public abstract class SendMessageEntry {
    public abstract String getBody();

    public abstract Optional<Duration> getDelay();

    @Default
    public SingleSubject<String> getResultSubject() {
        return SingleSubject.create();
    }

    public static ImmutableSendMessageEntry.Builder builder() {
        return ImmutableSendMessageEntry.builder();
    }
}
