package com.bandwidth.sqs.actions;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.transform.CreateQueueRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.CreateQueueResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.client.SqsClient;
import com.bandwidth.sqs.queue.SqsQueueConfig;


public class CreateQueueAction extends SqsAwsSdkAction<CreateQueueRequest, CreateQueueResult> {

    public CreateQueueAction(SqsQueueConfig config) {
        super(createRequest(config), SqsClient.getSqsHostForRegion(config.getRegion()),
                new CreateQueueRequestMarshaller(),
                new CreateQueueResultStaxUnmarshaller());
    }

    @VisibleForTesting
    static CreateQueueRequest createRequest(SqsQueueConfig config) {
        //TODO: add deadletter config

        return new CreateQueueRequest()
                .withQueueName(config.getName())
                .withAttributes(config.getAttributes().getStringMap());
    }
}
