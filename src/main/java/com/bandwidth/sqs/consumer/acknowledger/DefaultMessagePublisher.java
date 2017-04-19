package com.bandwidth.sqs.consumer.acknowledger;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.bandwidth.sqs.client.SqsAsyncIoClient;

import java.time.Duration;

import io.reactivex.Completable;


public class DefaultMessagePublisher implements MessagePublisher<Message> {

    private final SqsAsyncIoClient sqsClient;

    public DefaultMessagePublisher(SqsAsyncIoClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    @Override
    public Completable publishMessage(Message newMessage, String newQueueUrl, Duration delay) {
        SendMessageRequest sendRequest = new SendMessageRequest()
                .withDelaySeconds((int) delay.getSeconds())
                .withMessageAttributes(newMessage.getMessageAttributes())
                .withMessageBody(newMessage.getBody())
                .withQueueUrl(newQueueUrl);

        return sqsClient.sendMessage(sendRequest).toCompletable();
    }
}