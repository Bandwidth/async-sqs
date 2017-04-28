package com.bandwidth.sqs.client;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.bandwidth.sqs.actions.CreateQueueAction;
import com.bandwidth.sqs.actions.GetQueueUrlAction;
import com.bandwidth.sqs.queue.DefaultSqsQueue;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.SqsQueueConfig;
import com.bandwidth.sqs.request_sender.SqsRequestSender;

import java.util.Optional;

import io.reactivex.Single;

public class DefaultSqsClient implements SqsClient {

    private static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExists";

    private final SqsRequestSender requestSender;

    public DefaultSqsClient(SqsRequestSender requestSender) {
        this.requestSender = requestSender;
    }

    @Override
    public Single<SqsQueue<String>> getQueueFromName(String queueName, Regions region,
            SqsQueueClientConfig clientConfig) {
        GetQueueUrlAction action = new GetQueueUrlAction(queueName, region);
        return requestSender.sendRequest(action)
                .map(GetQueueUrlResult::getQueueUrl)
                .map(url -> getQueueFromUrl(url, clientConfig));
    }

    @Override
    public SqsQueue<String> getQueueFromUrl(String queueUrl, SqsQueueClientConfig clientConfig) {
        return new DefaultSqsQueue(queueUrl, requestSender, clientConfig, Optional.empty());
    }

    @Override
    public Single<SqsQueue<String>> upsertQueue(SqsQueueConfig queueConfig, SqsQueueClientConfig clientConfig) {
        CreateQueueAction action = new CreateQueueAction(queueConfig);
        Single<SqsQueue<String>> output = requestSender.sendRequest(action).map(createQueueResult ->
                new DefaultSqsQueue(createQueueResult.getQueueUrl(), requestSender, clientConfig,
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
}
