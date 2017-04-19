package com.bandwidth.sqs.consumer.acknowledger;

import java.time.Duration;

import io.reactivex.Completable;

public interface MessagePublisher<T> {
    Completable publishMessage(T newMessage, String newQueueUrl, Duration delay);
}
