package com.bandwidth.sqs.publisher;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.Single;


public interface MessagePublisher<T> {
    /**
     * Publishes a message immediately, with the given delay
     *
     * @param message The message to publish
     * @param queueUrl The queue to send the message to
     * @param maybeDelay Amount of time a message is delayed before it can be consumed (Max 15 minutes)
     *                   or the default delay of the SQS queue if "empty"
     * @return
     */
    Single<SendMessageResult> publishMessage(T message, String queueUrl, Optional<Duration> maybeDelay);

    /**
     * Publishes a message with the default delay of the SQS queue
     *
     * @param message The message to publish
     * @param queueUrl The queue to send the message to
     * @return
     */
    default Single<SendMessageResult> publishMessage(T message, String queueUrl) {
        return publishMessage(message, queueUrl, Optional.empty());
    }
}
