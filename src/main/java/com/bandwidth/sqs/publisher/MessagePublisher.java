package com.bandwidth.sqs.publisher;

import com.amazonaws.services.sqs.model.SendMessageResult;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.Single;

public interface MessagePublisher<T> {
    Single<SendMessageResult> publishMessage(T newMessage, String newQueueUrl, Optional<Duration> delay);
}
