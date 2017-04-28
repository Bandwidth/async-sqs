package com.bandwidth.sqs.queue.entry;


import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

import io.reactivex.subjects.CompletableSubject;

@Immutable
public abstract class ChangeMessageVisibilityEntry {

    public abstract String getReceiptHandle();

    public abstract Duration getNewVisibilityTimeout();

    @Default
    public CompletableSubject getResultSubject() {
        return CompletableSubject.create();
    }

    public static ImmutableChangeMessageVisibilityEntry.Builder builder() {
        return ImmutableChangeMessageVisibilityEntry.builder();
    }
}
