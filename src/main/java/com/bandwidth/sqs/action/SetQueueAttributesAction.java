package com.bandwidth.sqs.action;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.model.transform.SetQueueAttributesRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.SetQueueAttributesResultStaxUnmarshaller;
import com.bandwidth.sqs.action.adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.action.adapter.SqsAwsSdkBatchAction;
import com.bandwidth.sqs.queue.MutableSqsQueueAttributes;


public class SetQueueAttributesAction
        extends SqsAwsSdkBatchAction<SetQueueAttributesRequest, SetQueueAttributesResult> {

    public SetQueueAttributesAction(String queueUrl, MutableSqsQueueAttributes attributes) {
        super(createRequest(queueUrl, attributes), queueUrl,
                new SetQueueAttributesRequestMarshaller(),
                new SetQueueAttributesResultStaxUnmarshaller());
    }

    @VisibleForTesting
    static SetQueueAttributesRequest createRequest(String queueUrl, MutableSqsQueueAttributes attributes) {
        return new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .withAttributes(attributes.getStringMap());
    }
}
