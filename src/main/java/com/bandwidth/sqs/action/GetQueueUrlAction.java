package com.bandwidth.sqs.action;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.transform.GetQueueUrlRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.GetQueueUrlResultStaxUnmarshaller;
import com.bandwidth.sqs.action.adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.client.SqsClient;

public class GetQueueUrlAction extends SqsAwsSdkAction<GetQueueUrlRequest, GetQueueUrlResult> {
    public static final boolean IS_BATCH_ACTION = false;

    public GetQueueUrlAction(String queueName, Regions region) {
        super(createRequest(queueName), SqsClient.getSqsHostForRegion(region),
                new GetQueueUrlRequestMarshaller(),
                new GetQueueUrlResultStaxUnmarshaller(),
                IS_BATCH_ACTION);
    }

    @VisibleForTesting
    static GetQueueUrlRequest createRequest(String queueName) {
        return new GetQueueUrlRequest().withQueueName(queueName);

    }
}
