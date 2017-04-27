package com.bandwidth.sqs.actions;

import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.transform.DeleteMessageBatchRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.DeleteMessageBatchResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.aws_sdk_adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.queue.buffer.entry.DeleteMessageEntry;

import java.util.Map;
import java.util.stream.Collectors;

public class DeleteMessageBatchAction extends SqsAwsSdkAction<DeleteMessageBatchRequest, DeleteMessageBatchResult> {
    public DeleteMessageBatchAction(String queueUrl, Map<String, DeleteMessageEntry> entries) {
        super(createRequest(queueUrl, entries), queueUrl, new DeleteMessageBatchRequestMarshaller(),
                new DeleteMessageBatchResultStaxUnmarshaller());
    }

    public static DeleteMessageBatchRequest createRequest(String queueUrl, Map<String, DeleteMessageEntry> entries) {
        System.out.println("Deleting " + entries.size() + " messages");
        return new DeleteMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(entries.entrySet().stream()
                        .map(keyValue -> new DeleteMessageBatchRequestEntry()
                                .withId(keyValue.getKey())
                                .withReceiptHandle(keyValue.getValue().getReceiptHandle())
                        ).collect(Collectors.toList()));
    }
}
