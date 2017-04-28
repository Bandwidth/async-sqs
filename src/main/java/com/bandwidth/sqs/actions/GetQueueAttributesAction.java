package com.bandwidth.sqs.actions;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.transform.GetQueueAttributesRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.GetQueueAttributesResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.adapter.SqsAwsSdkAction;

import java.util.Collections;

public class GetQueueAttributesAction extends SqsAwsSdkAction<GetQueueAttributesRequest, GetQueueAttributesResult> {

    public GetQueueAttributesAction(String queueUrl) {
        super(createRequest(queueUrl), queueUrl,
                new GetQueueAttributesRequestMarshaller(),
                new GetQueueAttributesResultStaxUnmarshaller());
    }

    @VisibleForTesting
    static GetQueueAttributesRequest createRequest(String queueUrl) {
        return new GetQueueAttributesRequest(queueUrl, Collections.singletonList("All"));
    }
}
