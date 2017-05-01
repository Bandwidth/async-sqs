package com.bandwidth.sqs.actions;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.transform.DeleteMessageBatchRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.DeleteMessageBatchResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.queue.entry.DeleteMessageEntry;

import java.util.Map;
import java.util.stream.Collectors;

public class DeleteMessageBatchAction extends SqsAwsSdkAction<DeleteMessageBatchRequest, DeleteMessageBatchResult> {
    public DeleteMessageBatchAction(String queueUrl, Map<String, DeleteMessageEntry> entries) {
        super(createRequest(queueUrl, entries), queueUrl, new DeleteMessageBatchRequestMarshaller(),
                new DeleteMessageBatchResultStaxUnmarshaller());
    }

    @VisibleForTesting
    static DeleteMessageBatchRequest createRequest(String queueUrl, Map<String, DeleteMessageEntry> entries) {
        return new DeleteMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(entries.entrySet().stream()
                        .map(keyValue -> new DeleteMessageBatchRequestEntry()
                                .withId(keyValue.getKey())
                                .withReceiptHandle(keyValue.getValue().getReceiptHandle())
                        ).collect(Collectors.toList()));
    }
}
