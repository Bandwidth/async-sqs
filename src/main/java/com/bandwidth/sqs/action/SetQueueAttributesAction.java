package com.bandwidth.sqs.action;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.model.transform.SetQueueAttributesRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.SetQueueAttributesResultStaxUnmarshaller;
import com.bandwidth.sqs.action.adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.queue.SqsQueueAttributeChanges;

public class SetQueueAttributesAction extends SqsAwsSdkAction<SetQueueAttributesRequest, SetQueueAttributesResult> {
    public SetQueueAttributesAction(String queueUrl, SqsQueueAttributeChanges attributeChanges) {
        super(createRequest(queueUrl, attributeChanges), queueUrl,
                new SetQueueAttributesRequestMarshaller(),
                new SetQueueAttributesResultStaxUnmarshaller());
    }

    @VisibleForTesting
    static SetQueueAttributesRequest createRequest(String queueUrl, SqsQueueAttributeChanges attributeChanges) {
        return new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .withAttributes(attributeChanges.getStringMap());
    }
}
