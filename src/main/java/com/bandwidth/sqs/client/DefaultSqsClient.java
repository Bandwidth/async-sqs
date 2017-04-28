package com.bandwidth.sqs.client;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.bandwidth.sqs.actions.CreateQueueAction;
import com.bandwidth.sqs.actions.GetQueueUrlAction;
import com.bandwidth.sqs.queue.SerializingSqsQueue;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueueConfig;
import com.bandwidth.sqs.queue.StringSqsQueue;
import com.bandwidth.sqs.request_sender.SqsRequestSender;

import java.util.Optional;

import io.reactivex.Single;
import io.reactivex.functions.Function;

public class DefaultSqsClient<T> implements SqsClient<T> {

    private static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExists";

    private final SqsRequestSender requestSender;
    public final Function<String, T> deserialize;
    public final Function<T, String> serialize;

    public DefaultSqsClient(SqsRequestSender requestSender, Function<String, T> deserialize,
            Function<T, String> serialize) {
        this.requestSender = requestSender;
        this.deserialize = deserialize;
        this.serialize = serialize;
    }

    @Override
    public Single<SqsQueue<T>> getQueueFromName(String queueName, Regions region,
            SqsQueueClientConfig clientConfig) {
        GetQueueUrlAction action = new GetQueueUrlAction(queueName, region);
        return requestSender.sendRequest(action)
                .map(GetQueueUrlResult::getQueueUrl)
                .map(url -> getQueueFromUrl(url, clientConfig));
    }

    @Override
    public SqsQueue<T> getQueueFromUrl(String queueUrl, SqsQueueClientConfig clientConfig) {
        SqsQueue<String> rawQueue = new StringSqsQueue(queueUrl, requestSender, clientConfig, Optional.empty());
        return new SerializingSqsQueue<>(rawQueue, deserialize, serialize);
    }

    @Override
    public Single<SqsQueue<T>> upsertQueue(SqsQueueConfig queueConfig, SqsQueueClientConfig clientConfig) {
        CreateQueueAction action = new CreateQueueAction(queueConfig);
        Single<SqsQueue<T>> output = requestSender.sendRequest(action).map(createQueueResult -> {
            SqsQueue<String> rawQueue = new StringSqsQueue(createQueueResult.getQueueUrl(), requestSender,
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
}
