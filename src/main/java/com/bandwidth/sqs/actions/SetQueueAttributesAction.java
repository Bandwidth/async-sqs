package com.bandwidth.sqs.actions;

import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.model.transform.SetQueueAttributesRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.SetQueueAttributesResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.aws_sdk_adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.queue.SqsQueueAttributes;

public class SetQueueAttributesAction extends SqsAwsSdkAction<SetQueueAttributesRequest, SetQueueAttributesResult> {
    public SetQueueAttributesAction(String queueUrl, SqsQueueAttributes attributes) {
        super(createRequest(queueUrl, attributes), queueUrl,
                new SetQueueAttributesRequestMarshaller(),
                new SetQueueAttributesResultStaxUnmarshaller());
    }

    public static SetQueueAttributesRequest createRequest(String queueUrl, SqsQueueAttributes attributes) {
        return new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .withAttributes(attributes.getStringMap());
    }
}
