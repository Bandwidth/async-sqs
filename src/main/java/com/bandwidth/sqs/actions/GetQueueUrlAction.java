package com.bandwidth.sqs.actions;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.transform.GetQueueUrlRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.GetQueueUrlResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.client.SqsClient;

public class GetQueueUrlAction extends SqsAwsSdkAction<GetQueueUrlRequest, GetQueueUrlResult> {

    public GetQueueUrlAction(String queueName, Regions region) {
        super(createRequest(queueName), SqsClient.getSqsHostForRegion(region),
                new GetQueueUrlRequestMarshaller(),
                new GetQueueUrlResultStaxUnmarshaller());
    }

    @VisibleForTesting
    static GetQueueUrlRequest createRequest(String queueName) {
        return new GetQueueUrlRequest().withQueueName(queueName);

    }
}
