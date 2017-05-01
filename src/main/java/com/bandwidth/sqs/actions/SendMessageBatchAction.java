package com.bandwidth.sqs.actions;

import com.google.common.annotations.VisibleForTesting;

import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.transform.SendMessageBatchRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.SendMessageBatchResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.adapter.SqsAwsSdkAction;
import com.bandwidth.sqs.queue.entry.SendMessageEntry;

import java.util.Map;
import java.util.stream.Collectors;

public class SendMessageBatchAction extends SqsAwsSdkAction<SendMessageBatchRequest, SendMessageBatchResult> {

    public SendMessageBatchAction(String queueUrl, Map<String, SendMessageEntry> entries) {
        super(createRequest(queueUrl, entries), queueUrl, new SendMessageBatchRequestMarshaller(),
                new SendMessageBatchResultStaxUnmarshaller()
        );
    }

    @VisibleForTesting
    static SendMessageBatchRequest createRequest(String queueUrl, Map<String, SendMessageEntry> entries) {
        return new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(entries.entrySet().stream().map(keyValue -> {
                            SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry()
                                    .withId(keyValue.getKey())
                                    .withMessageBody(keyValue.getValue().getBody());

                            keyValue.getValue().getDelay()
                                    .ifPresent((delay) -> entry.setDelaySeconds((int) delay.getSeconds()));

                            return entry;
                        }).collect(Collectors.toList())
                );
    }

}
