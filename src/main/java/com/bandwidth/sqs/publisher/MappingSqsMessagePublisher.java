package com.bandwidth.sqs.publisher;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.Single;
import io.reactivex.functions.Function;

public class MappingSqsMessagePublisher<T, U> implements SqsMessagePublisher<T> {

    private final Function<T, U> map;
    private final SqsMessagePublisher<U> delegate;

    public MappingSqsMessagePublisher(SqsMessagePublisher<U> delegate, Function<T, U> map) {
        this.map = map;
        this.delegate = delegate;
    }

    @Override
    public Single<String> publishMessage(T body, Optional<Duration> maybeDelay) {
        return Single.defer(() -> delegate.publishMessage(map.apply(body), maybeDelay));
    }
}
