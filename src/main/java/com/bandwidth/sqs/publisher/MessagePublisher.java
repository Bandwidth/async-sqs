package com.bandwidth.sqs.publisher;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.Single;


public interface MessagePublisher<T> {
    /**
     * Publishes a message immediately, with the given delay
     *
     * @param body The message body to publish
     * @param maybeDelay Amount of time a message is delayed before it can be consumed (Max 15 minutes)
     *                   or the default delay of the SQS queue if "empty"
     * @return The message id
     */
    Single<String> publishMessage(T body, Optional<Duration> maybeDelay);

    /**
     * Publishes a message with the default delay of the SQS queue
     *
     * @param body The message body to publish
     * @return The message id
     */
    default Single<String> publishMessage(T body) {
        return publishMessage(body, Optional.empty());
    }
}
