package com.bandwidth.sqs.queue;

import com.bandwidth.sqs.publisher.MessagePublisher;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.reactivex.Completable;
import io.reactivex.Single;

public interface SqsQueue<T> extends MessagePublisher<T> {

    int DEFAULT_MAX_RECEIVE_MESSAGES = 10;

    String getQueueUrl();

    Single<SqsQueueAttributes> getAttributes();

    Single<List<SqsMessage<T>>> receiveMessages(int maxMessages, Optional<Duration> waitTime,
            Optional<Duration> visibilityTimeout);

    Completable deleteMessage(String receiptHandle);

    Completable changeMessageVisibility(String receiptHandle, Duration newVisibility);

    Completable setAttributes(SqsQueueAttributes newAttributes);

    default Single<List<SqsMessage<T>>> receiveMessages(int maxMessages, Optional<Duration> waitTime) {
        return receiveMessages(maxMessages, waitTime, Optional.empty());
    }

    default Single<List<SqsMessage<T>>> receiveMessages(int maxMessages) {
        return receiveMessages(maxMessages, Optional.empty());
    }

    default Single<List<SqsMessage<T>>> receiveMessages() {
        return receiveMessages(DEFAULT_MAX_RECEIVE_MESSAGES);
    }

    default Completable deleteMessage(SqsMessage<?> message) {
        return deleteMessage(message.getReceiptHandle());
    }

    default Completable changeMessageVisibility(SqsMessage<?> message, Duration newVisibility) {
        return changeMessageVisibility(message.getReceiptHandle(), newVisibility);
    }
}
