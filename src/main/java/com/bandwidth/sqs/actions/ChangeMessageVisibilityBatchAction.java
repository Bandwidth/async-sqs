package com.bandwidth.sqs.actions;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.transform.ChangeMessageVisibilityBatchRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.ChangeMessageVisibilityBatchResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.aws_sdk_adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.queue.buffer.entry.ChangeMessageVisibilityEntry;

import java.util.Map;
import java.util.stream.Collectors;

public class ChangeMessageVisibilityBatchAction
        extends SqsAwsSdkAction<ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult>{

    public ChangeMessageVisibilityBatchAction(String queueUrl, Map<String, ChangeMessageVisibilityEntry> entries) {
        super(createRequest(queueUrl, entries), queueUrl,
                new ChangeMessageVisibilityBatchRequestMarshaller(),
                new ChangeMessageVisibilityBatchResultStaxUnmarshaller());
    }

    public static ChangeMessageVisibilityBatchRequest createRequest(String queueUrl, Map<String,
            ChangeMessageVisibilityEntry> entries) {

        return new ChangeMessageVisibilityBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(entries.entrySet().stream()
                        .map(keyValue -> new ChangeMessageVisibilityBatchRequestEntry()
                                .withId(keyValue.getKey())
                                .withReceiptHandle(keyValue.getValue().getReceiptHandle())
                                .withVisibilityTimeout((int) keyValue.getValue().getNewVisibilityTimeout().getSeconds())
                        ).collect(Collectors.toList()));
    }
}
