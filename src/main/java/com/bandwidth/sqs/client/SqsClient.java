package com.bandwidth.sqs.client;

import com.amazonaws.regions.Regions;
import com.bandwidth.sqs.queue.ImmutableSqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueueConfig;

import java.text.MessageFormat;

import io.reactivex.Single;

public interface SqsClient {

    /**
     * @param queueName    Name of the SQS queue. This queue must already exist, it will not be created.
     * @param region       Region this queue exists in
     * @param clientConfig Configuration values for the queue client
     * @return an SqsQueue
     */
    Single<SqsQueue<String>> getQueueFromName(String queueName, Regions region, SqsQueueClientConfig clientConfig);

    /**
     * Gets an SqsQueue object from a queueUrl. Note this does not do any HTTP requests and returns instantly.
     *
     * @param queueUrl     Full url of the SQS queue
     * @param clientConfig Configuration values for the queue client
     * @return an SqsQueue
     */
    SqsQueue<String> getQueueFromUrl(String queueUrl, SqsQueueClientConfig clientConfig);

    /**
     * Asserts that an SQS queue exists with specific attributes. The queue is created if it does not exist,
     * and any existing queue is modified if the attributes don't match.
     *
     * @param queueConfig  Configuration of the SQS queue
     * @param clientConfig Configuration of the SQS client
     * @return an SqsQueue
     */
    Single<SqsQueue<String>> upsertQueue(SqsQueueConfig queueConfig, SqsQueueClientConfig clientConfig);

    /**
     * @param queueName Name of the SQS queue. This queue must already exist, it will not be created.
     * @param region    Region this queue exists in
     * @return an SqsQueue
     */
    default Single<SqsQueue<String>> getQueueFromName(String queueName, Regions region) {
        return getQueueFromName(queueName, region, SqsQueueClientConfig.builder().build());
    }

    /**
     * Gets an SqsQueue object from a queueUrl. Note this does not do any HTTP requests and returns instantly.
     *
     * @param queueUrl Full url of the SQS queue
     * @return an SqsQueue
     */
    default SqsQueue<String> getQueueFromUrl(String queueUrl) {
        return getQueueFromUrl(queueUrl, SqsQueueClientConfig.builder().build());
    }

    /**
     * Asserts that an SQS queue exists with specific attributes. The queue is created if it does not exist,
     * and any existing queue is modified if the attributes don't match.
     *
     * @param queueConfig Configuration of the SQS queue
     * @return an SqsQueue
     */
    default Single<SqsQueue<String>> upsertQueue(SqsQueueConfig queueConfig) {
        return upsertQueue(queueConfig, ImmutableSqsQueueClientConfig.builder().build());
    }

    static String getSqsHostForRegion(Regions region) {
        return MessageFormat.format("https://sqs.{0}.amazonaws.com/", region.getName());
    }

    static SqsClientBuilder builder() {
        return new SqsClientBuilder();
    }
}
