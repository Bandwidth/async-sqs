package com.bandwidth.sqs.client;

import io.reactivex.subjects.SingleSubject;

/**
 * @param <E> Request(E)ntry
 * @param <S> MessageRe(S)ult
 */
public class BatchRequestEntry<E, S> {
    private final E requestEntry;
    private final SingleSubject<S> singleSubject;

    public BatchRequestEntry(E requestEntry, SingleSubject<S> singleSubject) {
        this.requestEntry = requestEntry;
        this.singleSubject = singleSubject;
    }

    public E getRequestEntry() {
        return requestEntry;
    }

    public void onSuccess(S result) {
        singleSubject.onSuccess(result);
    }

    public void onError(Throwable exception) {
        singleSubject.onError(exception);
    }

}