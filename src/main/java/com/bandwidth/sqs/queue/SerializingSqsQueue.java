package com.bandwidth.sqs.queue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public class SerializingSqsQueue<A, B> implements SqsQueue<B> {

    private final SqsQueue<A> delegate;
    private final Function<A, B> deserialize;
    private final Function<B, A> serialize;


    public SerializingSqsQueue(SqsQueue<A> delegate, Function<A, B> deserialize, Function<B, A> serialize) {
        this.delegate = delegate;
        this.deserialize = deserialize;
        this.serialize = serialize;
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
    public Single<List<SqsMessage<B>>> receiveMessages(int maxMessages, Optional<Duration> waitTime,
            Optional<Duration> visibilityTimeout) {
        return delegate.receiveMessages(maxMessages, waitTime, visibilityTimeout).map(sqsMessages -> {
            //can't stream/map here since deserialize could throw checked exception
            List<SqsMessage<B>> serializedList = new ArrayList<>();
            for (SqsMessage<A> sqsMessage : sqsMessages) {
                serializedList.add(SqsMessage.<B>builder()
                        .receiptHandle(sqsMessage.getReceiptHandle())
                        .receivedTime(sqsMessage.getReceivedTime())
                        .id(sqsMessage.getId())
                        .body(deserialize.apply(sqsMessage.getBody()))
                        .build()
                );
            }
            return serializedList;
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
    public Completable setAttributes(SqsQueueAttributes newAttributes) {
        return delegate.setAttributes(newAttributes);
    }

    @Override
    public Single<String> publishMessage(B body, Optional<Duration> maybeDelay) {
        return Single.defer(() -> {
            A serializedBody = serialize.apply(body);
            return delegate.publishMessage(serializedBody, maybeDelay);
        });
    }
}
