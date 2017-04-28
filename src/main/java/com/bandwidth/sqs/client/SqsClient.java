package com.bandwidth.sqs.client;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.bandwidth.sqs.actions.CreateQueueAction;
import com.bandwidth.sqs.actions.GetQueueUrlAction;
import com.bandwidth.sqs.queue.buffer.BufferedSqsQueue;
import com.bandwidth.sqs.queue.ImmutableSqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueueConfig;
import com.bandwidth.sqs.actions.sender.SqsRequestSender;

import java.text.MessageFormat;
import java.util.Optional;

import io.reactivex.Single;

public class SqsClient {

    private static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExists";

    private final SqsRequestSender requestSender;

    public SqsClient(SqsRequestSender requestSender) {
        this.requestSender = requestSender;
    }

    /**
     * @param queueName    Name of the SQS queue. This queue must already exist, it will not be created.
     * @param region       Region this queue exists in
     * @param clientConfig Configuration values for the queue client
     * @return an SqsQueue
     */
    public Single<SqsQueue<String>> getQueueFromName(String queueName, Regions region,
            SqsQueueClientConfig clientConfig) {
        GetQueueUrlAction action = new GetQueueUrlAction(queueName, region);
        return requestSender.sendRequest(action)
                .map(GetQueueUrlResult::getQueueUrl)
                .map(url -> getQueueFromUrl(url, clientConfig));
    }

    /**
     * Asserts that an SQS queue exists with specific attributes. The queue is created if it does not exist,
     * and any existing queue is modified if the attributes don't match.
     *
     * @param queueConfig  Configuration of the SQS queue
     * @param clientConfig Configuration of the SQS client
     * @return an SqsQueue
     */
    public Single<SqsQueue<String>> upsertQueue(SqsQueueConfig queueConfig, SqsQueueClientConfig clientConfig) {
        CreateQueueAction action = new CreateQueueAction(queueConfig);
        Single<SqsQueue<String>> output = requestSender.sendRequest(action).map(createQueueResult ->
                new BufferedSqsQueue(createQueueResult.getQueueUrl(), requestSender, clientConfig,
                        Optional.of(queueConfig.getAttributes()))
        );
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
     * @param queueConfig Configuration of the SQS queue
     * @return an SqsQueue
     */
    public Single<SqsQueue<String>> upsertQueue(SqsQueueConfig queueConfig) {
        return upsertQueue(queueConfig, ImmutableSqsQueueClientConfig.builder().build());
    }

    public static String getSqsHostForRegion(Regions region) {
        return MessageFormat.format("https://sqs.{0}.amazonaws.com/", region.getName());
    }

    public static SqsClientBuilder builder() {
        return new SqsClientBuilder();
    }

    private SqsQueue<String> getQueueFromUrl(String queueUrl, SqsQueueClientConfig clientConfig) {
        return new BufferedSqsQueue(queueUrl, requestSender, clientConfig, Optional.empty());
    }
}
