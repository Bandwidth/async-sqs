package com.bandwidth.sqs.client;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.bandwidth.sqs.queue.MappingSqsQueue;
import com.bandwidth.sqs.queue.RetryingSqsQueue;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueueConfig;
import com.bandwidth.sqs.queue.buffer.BufferedStringSqsQueue;
import com.bandwidth.sqs.action.CreateQueueAction;
import com.bandwidth.sqs.action.GetQueueUrlAction;
import com.bandwidth.sqs.action.sender.SqsRequestSender;

import java.text.MessageFormat;

import io.reactivex.Single;
import io.reactivex.functions.Function;

public class SqsClient {

    private static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExists";

    private final SqsRequestSender requestSender;
    private final int retryCount;

    public SqsClient(SqsRequestSender requestSender, int retryCount) {
        this.requestSender = requestSender;
        this.retryCount = retryCount;
    }

    /**
     * @param queueName    Name of the SQS queue. This queue must already exist, it will not be created.
     * @param region       Region this queue exists in
     * @param clientConfig Configuration values for the queue client
     * @return an SqsQueue
     */
    public Single<SqsQueue<String>> getQueueFromName(String queueName, Regions region, SqsQueueClientConfig
            clientConfig) {
        GetQueueUrlAction action = new GetQueueUrlAction(queueName, region);
        return requestSender.sendRequest(action)
                .map(GetQueueUrlResult::getQueueUrl)
                .map(url -> getQueueFromUrl(url, clientConfig));
    }

    /**
     * @param queueName Name of the SQS queue. This queue must already exist, it will not be created.
     * @param region    Region this queue exists in
     * @return an SqsQueue
     */
    public Single<SqsQueue<String>> getQueueFromName(String queueName, Regions region) {
        return getQueueFromName(queueName, region, SqsQueueClientConfig.builder().build());
    }

    /**
     * Asserts that an SQS queue exists with specific attributes. The queue is created if it does not exist,
     * and any existing queue is modified if the attributes don't match.
     *
     * @param queueConfig  Configuration of the SQS queue
     * @param clientConfig Configuration of the SQS queue client
     * @return an SqsQueue
     */
    public Single<SqsQueue<String>> upsertQueue(SqsQueueConfig queueConfig, SqsQueueClientConfig clientConfig) {
        CreateQueueAction action = new CreateQueueAction(queueConfig);
        Single<SqsQueue<String>> output = requestSender.sendRequest(action)
                .map(createQueueResult -> getQueueFromUrl(createQueueResult.getQueueUrl(), clientConfig));
        return output.onErrorResumeNext((err) -> {
            if (err instanceof AmazonSQSException) {
                AmazonSQSException awsException = (AmazonSQSException) err;
                //Queue already exists, but has wrong attributes. We need to update them.
                if (QUEUE_ALREADY_EXISTS.equals(awsException.getErrorCode())) {
                    //Have to get queue from name since we don't know the url yet.
                    return getQueueFromName(queueConfig.getName(), queueConfig.getRegion(), clientConfig)
                            .flatMap((queue) -> {
                                return queue.setAttributes(queueConfig.getAttributes()).toSingleDefault(queue);
                            });
                }
            }
            return Single.error(err);
        });
    }

    /**
     * Asserts that an SQS queue exists with specific attributes. The queue is created if it does not exist,
     * and any existing queue is modified if the attributes don't match.
     *
     * @param queueConfig Configuration of the SQS queue
     * @return an SqsQueue
     */
    public Single<SqsQueue<String>> upsertQueue(SqsQueueConfig queueConfig) {
        return upsertQueue(queueConfig, SqsQueueClientConfig.builder().build());
    }

    public static SqsClientBuilder builder() {
        return new SqsClientBuilder();
    }

    public static String getSqsHostForRegion(Regions region) {
        return MessageFormat.format("https://sqs.{0}.amazonaws.com/", region.getName());
    }

    private SqsQueue<String> getQueueFromUrl(String queueUrl, SqsQueueClientConfig clientConfig) {
        BufferedStringSqsQueue bufferedQueue = new BufferedStringSqsQueue(queueUrl, requestSender, clientConfig);
        return new RetryingSqsQueue<>(bufferedQueue, retryCount);
    }
}
