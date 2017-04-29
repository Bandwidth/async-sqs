package com.bandwidth.sqs.consumer;

import com.bandwidth.sqs.consumer.handler.ConsumerHandler;
import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;
import com.bandwidth.sqs.consumer.strategy.backoff.NullBackoffStrategy;
import com.bandwidth.sqs.consumer.strategy.expiration.ExpirationStrategy;
import com.bandwidth.sqs.consumer.strategy.expiration.NeverExpiresStrategy;
import com.bandwidth.sqs.queue.SqsQueue;

public class SqsConsumerBuilder {
    public static final int DEFAULT_NUM_PERMITS = 500;
    public static final int DEFAULT_BUFFER_SIZE = 640;

    final SqsConsumerManager consumerManager;
    final SqsQueue<String> sqsQueue;
    final ConsumerHandler<String> consumerHandler;

    int numPermits = DEFAULT_NUM_PERMITS;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    BackoffStrategy backoffStrategy = new NullBackoffStrategy();
    ExpirationStrategy expirationStrategy = new NeverExpiresStrategy();

    /**
     * @param manager         A SqsConsumerManager that manages interactions between all of the consumers
     * @param sqsQueue        The sqsQueue
     * @param consumerHandler A handler used to process a message. The number of current messages being processed by the
     *                        handler is limited by `numPermits`. You *MUST* either `ack()` or `nack()` EVERY message
     *                        using the provided MessageAcknowledger to allow more messages to be processed.
     */
    public SqsConsumerBuilder(SqsConsumerManager manager, SqsQueue<String> sqsQueue, ConsumerHandler<String>
            consumerHandler) {
        this.consumerManager = manager;
        this.sqsQueue = sqsQueue;
        this.consumerHandler = consumerHandler;

    }


    public SqsConsumer build() {
        return new SqsConsumer(this);
    }

    /**
     * @param numPermits Max number of concurrent requests this consumer can process. A permit is consumed when the
     *                   handler is called, and released when the message is acked or nacked.
     */
    public SqsConsumerBuilder withNumPermits(int numPermits) {
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
    public SqsConsumerBuilder withBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * @param backoffStrategy backoff strategy to use for this consumer
     */
    public SqsConsumerBuilder withBackoffStrategy(BackoffStrategy backoffStrategy) {
        this.backoffStrategy = backoffStrategy;
        return this;
    }

    /**
     * @param expirationStrategy Strategy to determine if a message has expired before it is processed
     */
    public SqsConsumerBuilder withExpirationStrategy(ExpirationStrategy expirationStrategy) {
        this.expirationStrategy = expirationStrategy;
        return this;
    }


}