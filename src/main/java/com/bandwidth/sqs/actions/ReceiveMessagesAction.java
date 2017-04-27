package com.bandwidth.sqs.actions;

import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.transform.ReceiveMessageRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.ReceiveMessageResultStaxUnmarshaller;
import com.bandwidth.sqs.actions.aws_sdk_adapter.SqsAwsSdkAction;

import java.time.Duration;
import java.util.Optional;

public class ReceiveMessagesAction extends SqsAwsSdkAction<ReceiveMessageRequest, ReceiveMessageResult> {
    public ReceiveMessagesAction(String queueUrl, int maxMessages, Optional<Duration> waitTime,
            Optional<Duration> visibilityTimeout) {

        super(createRequest(queueUrl, maxMessages, waitTime, visibilityTimeout),
                queueUrl,
                new ReceiveMessageRequestMarshaller(),
                new ReceiveMessageResultStaxUnmarshaller()
        );
    }

    public static ReceiveMessageRequest createRequest(String queueUrl, int maxMessages, Optional<Duration> waitTime,
            Optional<Duration> visibilityTimeout) {

        ReceiveMessageRequest request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(maxMessages);
        visibilityTimeout.ifPresent((duration) -> {
            request.setVisibilityTimeout((int) duration.getSeconds());
        });
        waitTime.ifPresent((duration) -> {
            request.setWaitTimeSeconds((int) duration.getSeconds());
        });
        return request;
    }
}
