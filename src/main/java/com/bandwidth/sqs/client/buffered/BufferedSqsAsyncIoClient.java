package com.bandwidth.sqs.client.buffered;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.bandwidth.sqs.client.BatchRequestEntry;
import com.bandwidth.sqs.client.SqsAsyncIoClient;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.subjects.SingleSubject;


public class BufferedSqsAsyncIoClient implements SqsAsyncIoClient {
    public static final int MAX_BATCH_SIZE = 10;

    private final KeyedTaskBuffer<String, BatchRequestEntry<SendMessageBatchRequestEntry, SendMessageResult>>
            sendMessageTaskBuffer;
    private final KeyedTaskBuffer<String, BatchRequestEntry<DeleteMessageBatchRequestEntry, DeleteMessageResult>>
            deleteMessageTaskBuffer;
    private final KeyedTaskBuffer<String, BatchRequestEntry<ChangeMessageVisibilityBatchRequestEntry,
            ChangeMessageVisibilityResult>> changeMessageVisibilityTaskBuffer;

    private final SqsAsyncIoClient delegate;

    public BufferedSqsAsyncIoClient(Duration maxWaitTime, SqsAsyncIoClient delegate) {
        this.delegate = delegate;
        this.sendMessageTaskBuffer =
                new KeyedTaskBuffer<>(MAX_BATCH_SIZE, maxWaitTime, new SendMessageTask(this));

        this.deleteMessageTaskBuffer =
                new KeyedTaskBuffer<>(MAX_BATCH_SIZE, maxWaitTime, new DeleteMessageTask(this));

        this.changeMessageVisibilityTaskBuffer =
                new KeyedTaskBuffer<>(MAX_BATCH_SIZE, maxWaitTime, new ChangeMessageVisibilityTask(this));
    }

    /**
     * Queues a request to delete a message. If the queue size reaches 10, or 'maxWaitMillis' has elapsed since the
     * first message entered the queue, all of the pending requests will be sent in a single batch request.
     */
    @Override
    public Single<DeleteMessageResult> deleteMessage(DeleteMessageRequest request) {
        DeleteMessageBatchRequestEntry entry = new DeleteMessageBatchRequestEntry()
                .withReceiptHandle(request.getReceiptHandle());
        SingleSubject<DeleteMessageResult> singleSubject = SingleSubject.create();
        deleteMessageTaskBuffer.addData(request.getQueueUrl(), new BatchRequestEntry<>(entry, singleSubject));
        return singleSubject;
    }

    /**
     * Queues a message to be sent. If the queue size reaches 10, or 'maxWaitMillis' has elapsed since the first message
     * entered the queue, all of the pending requests will be sent in a single batch request.
     */
    @Override
    public Single<String> publishMessage(Message message, String queueUrl, Optional<Duration> maybeDelay) {
        SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry()
                .withMessageAttributes(message.getMessageAttributes())
                .withMessageBody(message.getBody());
        maybeDelay.ifPresent((delay) -> entry.setDelaySeconds((int)delay.getSeconds()));

        SingleSubject<SendMessageResult> singleSubject = SingleSubject.create();
        sendMessageTaskBuffer.addData(queueUrl, new BatchRequestEntry<>(entry, singleSubject));
        return singleSubject.map(SendMessageResult::getMessageId);
    }

    /**
     * Queues a changeMessageVisibility request. If the queue size reaches 10, or 'maxWaitMillis' has elapsed since the
     * first request entered the queue, all of the pending requests will be sent in a single batch request.
     */
    @Override
    public Single<ChangeMessageVisibilityResult> changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        ChangeMessageVisibilityBatchRequestEntry entry = new ChangeMessageVisibilityBatchRequestEntry()
                .withReceiptHandle(request.getReceiptHandle())
                .withVisibilityTimeout(request.getVisibilityTimeout());
        SingleSubject<ChangeMessageVisibilityResult> singleSubject = SingleSubject.create();
        changeMessageVisibilityTaskBuffer.addData(request.getQueueUrl(), new BatchRequestEntry<>(entry, singleSubject));
        return singleSubject;
    }

    /**
     * Immediately sends a request to retrieve messages.
     */
    @Override
    public Single<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request) {
        return delegate.receiveMessage(request);
    }

    /**
     * Immediately deletes a batch of messages. In general `deleteMessage()` should be used instead unless you have
     * a full batch of requests ready to go.
     */
    @Override
    public Single<DeleteMessageBatchResult> deleteMessageBatch(DeleteMessageBatchRequest request) {
        return delegate.deleteMessageBatch(request);
    }

    /**
     * Immediately sends a batch of messages. In general `sendMessage()` should be used instead unless you have
     * a full batch of requests ready to go.
     */
    @Override
    public Single<SendMessageBatchResult> sendMessageBatch(SendMessageBatchRequest request) {
        return delegate.sendMessageBatch(request);
    }

    /**
     * Immediately sends a batch of changeMessageVisibility requests. In general `changeMessageVisibility()` should be
     * used instead unless you have a full batch of requests ready to go.
     */
    @Override
    public Single<ChangeMessageVisibilityBatchResult>
            changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        return delegate.changeMessageVisibilityBatch(request);
    }

}