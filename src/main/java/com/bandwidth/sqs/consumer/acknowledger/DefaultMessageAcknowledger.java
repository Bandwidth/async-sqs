package com.bandwidth.sqs.consumer.acknowledger;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.bandwidth.sqs.client.SqsAsyncIoClient;

import java.time.Duration;

import io.reactivex.Completable;


public class DefaultMessageAcknowledger extends MessageAcknowledger<Message> {

    public DefaultMessageAcknowledger(SqsAsyncIoClient sqsClient, String queueUrl, String receiptId) {
        super(sqsClient, queueUrl, receiptId);
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