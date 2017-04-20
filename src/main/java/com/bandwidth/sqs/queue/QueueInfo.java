package com.bandwidth.sqs.queue;

import java.time.Duration;
import java.util.Optional;

import jdk.nashorn.internal.ir.annotations.Immutable;

@Immutable
public interface QueueInfo {

    /**
     * SQS queue name, must be unique within an AWS account
     */
    String getName();

    /**
     * 0 - 12 hours, in 1 second increments
     */
    Duration getVisibilityTimeout();

    /**
     * 1 - 256KB, in KB
     */
    int getMaxMessageSize();

    /**
     * 0 - 15 minutes, in 1 second increments
     * @return
     */
    Duration getMessageDelay();

    /**
     * Name of dead-letter queue, if it should exist
     */
    Optional<String> getDeadletterQueueName();
}
