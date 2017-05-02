package com.bandwidth.sqs.queue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public class MappingSqsQueue<T, U> implements SqsQueue<U> {

    private final SqsQueue<T> delegate;
    private final Function<T, U> map;
    private final Function<U, T> inverseMap;


    public MappingSqsQueue(SqsQueue<T> delegate, Function<T, U> map, Function<U, T> inverseMap) {
        this.delegate = delegate;
        this.map = map;
        this.inverseMap = inverseMap;
    }

    @Override
    public String getQueueUrl() {
        return delegate.getQueueUrl();
    }

    @Override
    public Single<SqsQueueAttributes> getAttributes() {
        return delegate.getAttributes();
    }

    @Override
    public Single<List<SqsMessage<U>>> receiveMessages(int maxMessages, Optional<Duration> waitTime,
            Optional<Duration> visibilityTimeout) {
        return delegate.receiveMessages(maxMessages, waitTime, visibilityTimeout).map(sqsMessages -> {
            //can't stream/map here since map could throw checked exception
            List<SqsMessage<U>> mappedList = new ArrayList<>();
            for (SqsMessage<T> sqsMessage : sqsMessages) {
                mappedList.add(SqsMessage.<U>builder()
                        .receiptHandle(sqsMessage.getReceiptHandle())
                        .receivedTime(sqsMessage.getReceivedTime())
                        .id(sqsMessage.getId())
                        .body(map.apply(sqsMessage.getBody()))
                        .build()
                );
            }
            return mappedList;
        });
    }

    @Override
    public Completable deleteMessage(String receiptHandle) {
        return delegate.deleteMessage(receiptHandle);
    }

    @Override
    public Completable changeMessageVisibility(String receiptHandle, Duration newVisibility) {
        return delegate.changeMessageVisibility(receiptHandle, newVisibility);
    }

    @Override
    public Completable setAttributes(MutableSqsQueueAttributes attributes) {
        return delegate.setAttributes(attributes);
    }

    @Override
    public Single<String> publishMessage(U body, Optional<Duration> maybeDelay) {
        return Single.defer(() -> {
            T serializedBody = inverseMap.apply(body);
            return delegate.publishMessage(serializedBody, maybeDelay);
        });
    }
}
