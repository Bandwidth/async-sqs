package com.bandwidth.sqs.client;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.bandwidth.sqs.queue.SerializingSqsQueue;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueueConfig;
import com.bandwidth.sqs.queue.buffer.BufferedStringSqsQueue;
import com.bandwidth.sqs.action.CreateQueueAction;
import com.bandwidth.sqs.action.GetQueueUrlAction;
import com.bandwidth.sqs.action.sender.SqsRequestSender;

import java.text.MessageFormat;
import java.util.Optional;

import io.reactivex.Single;
import io.reactivex.functions.Function;

public class SqsClient<T> {

    private static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExists";

    private final SqsRequestSender requestSender;
    public final Function<String, T> deserialize;
    public final Function<T, String> serialize;

    public SqsClient(SqsRequestSender requestSender, Function<String, T> deserialize,
            Function<T, String> serialize) {
        this.requestSender = requestSender;
        this.deserialize = deserialize;
        this.serialize = serialize;
    }

    /**
     * @param queueName    Name of the SQS queue. This queue must already exist, it will not be created.
     * @param region       Region this queue exists in
     * @param clientConfig Configuration values for the queue client
     * @return an SqsQueue
     */
    public Single<SqsQueue<T>> getQueueFromName(String queueName, Regions region,
            SqsQueueClientConfig clientConfig) {
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
    public Single<SqsQueue<T>> getQueueFromName(String queueName, Regions region) {
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
    public Single<SqsQueue<T>> upsertQueue(SqsQueueConfig queueConfig, SqsQueueClientConfig clientConfig) {
        CreateQueueAction action = new CreateQueueAction(queueConfig);
        Single<SqsQueue<T>> output = requestSender.sendRequest(action).map(createQueueResult -> {
            SqsQueue<String> rawQueue = new BufferedStringSqsQueue(createQueueResult.getQueueUrl(), requestSender,
                    clientConfig, Optional.of(queueConfig.getAttributes()));
            return new SerializingSqsQueue<>(rawQueue, deserialize, serialize);
        });
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
    public Single<SqsQueue<T>> upsertQueue(SqsQueueConfig queueConfig) {
        return upsertQueue(queueConfig, SqsQueueClientConfig.builder().build());
    }

    /**
     * Returns a String SqsClient builder, but can be converted to any type of
     * SqsClient by providing your own serializer / deserializer
     */
    public static SqsClientBuilder<String> builder() {
        //default builder has "no-op" serializers
        return new SqsClientBuilder<>((str) -> str, (str) -> str);
    }

    public static String getSqsHostForRegion(Regions region) {
        return MessageFormat.format("https://sqs.{0}.amazonaws.com/", region.getName());
    }

    private SqsQueue<T> getQueueFromUrl(String queueUrl, SqsQueueClientConfig clientConfig) {
        SqsQueue<String> rawQueue = new BufferedStringSqsQueue(queueUrl, requestSender, clientConfig, Optional.empty());
        return new SerializingSqsQueue<>(rawQueue, deserialize, serialize);
    }
}
