package com.bandwidth.sqs.consumer;

import com.bandwidth.sqs.consumer.handler.ConsumerHandler;
import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;
import com.bandwidth.sqs.consumer.strategy.backoff.NullBackoffStrategy;
import com.bandwidth.sqs.consumer.strategy.expiration.ExpirationStrategy;
import com.bandwidth.sqs.consumer.strategy.expiration.NeverExpiresStrategy;
import com.bandwidth.sqs.queue.SqsQueue;

public class SqsConsumerBuilder<T> {
    public static final int DEFAULT_NUM_PERMITS = 500;
    public static final int DEFAULT_BUFFER_SIZE = 640;
    public static final int DEFAULT_PRIORITY = 0;

    final SqsConsumerManager consumerManager;
    final SqsQueue<T> sqsQueue;
    final ConsumerHandler<T> consumerHandler;

    int numPermits = DEFAULT_NUM_PERMITS;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    int priority = DEFAULT_PRIORITY;
    BackoffStrategy backoffStrategy = new NullBackoffStrategy();
    ExpirationStrategy expirationStrategy = new NeverExpiresStrategy();

    /**
     * @param manager         A SqsConsumerManager that manages interactions between all of the consumers
     * @param sqsQueue        The sqsQueue
     * @param consumerHandler A handler used to process a message. The number of current messages being processed by the
     *                        handler is limited by `numPermits`. You *MUST* either `ack()` or `nack()` EVERY message
     *                        using the provided MessageAcknowledger to allow more messages to be processed.
     */
    public SqsConsumerBuilder(SqsConsumerManager manager, SqsQueue<T> sqsQueue, ConsumerHandler<T> consumerHandler) {
        this.consumerManager = manager;
        this.sqsQueue = sqsQueue;
        this.consumerHandler = consumerHandler;
    }

    public SqsConsumer<T> build() {
        return new SqsConsumer<>(this);
    }

    /**
     * @param numPermits Max number of concurrent requests this consumer can process. A permit is consumed when the
     *                   handler is called, and released when the message is acked or nacked.
     */
    public SqsConsumerBuilder<T> withNumPermits(int numPermits) {
        this.numPermits = numPermits;
        return this;
    }

    /**
     * @param bufferSize size of the consumer messageBuffer. Ideally this should be slightly higher than the number of
     *                   messages that can be processed by this consumer in the amount of time needed to do an HTTP
     *                   round-trip to SQS. If this value is too low, performance may be lower than expected since a
     *                   small messageBuffer size limits the number of possible in-flight SQS requests. If you use a
     *                   value significantly too large, messages may start to reach their visibilityTimeout before
     *                   processing finishes causing duplicate messages. The maximum number of in-flight HTTP requests
     *                   is limited to max(1, floor(maxQueueSize/10))
     */
    public SqsConsumerBuilder<T> withBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * @param backoffStrategy backoff strategy to use for this consumer
     */
    public SqsConsumerBuilder<T> withBackoffStrategy(BackoffStrategy backoffStrategy) {
        this.backoffStrategy = backoffStrategy;
        return this;
    }

    /**
     * @param expirationStrategy Strategy to determine if a message has expired before it is processed
     */
    public SqsConsumerBuilder<T> withExpirationStrategy(ExpirationStrategy expirationStrategy) {
        this.expirationStrategy = expirationStrategy;
        return this;
    }

    /**
     * @param priority A consumer with a higher priority (lower value) will have messages processed first
     *                 (0 is the highest priority)
     */
    public SqsConsumerBuilder<T> withPriority(int priority){
        this.priority = priority;
        return this;
    }
}