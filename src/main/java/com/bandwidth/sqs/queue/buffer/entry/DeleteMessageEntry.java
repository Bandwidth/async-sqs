package com.bandwidth.sqs.queue.buffer.entry;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import io.reactivex.subjects.CompletableSubject;

@Immutable
public abstract class DeleteMessageEntry {
    public abstract String getReceiptHandle();

    @Default
    public CompletableSubject getResultSubject() {
        return CompletableSubject.create();
    }

    public static ImmutableDeleteMessageEntry.Builder builder() {
        return ImmutableDeleteMessageEntry.builder();
    }
}
