package com.bandwidth.sqs.client;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import io.reactivex.Single;
import io.reactivex.subjects.SingleSubject;

public class RetryingSqsAsyncIoClient implements SqsAsyncIoClient {

    private final int retryCount;
    private final SqsAsyncIoClient delegate;

    public RetryingSqsAsyncIoClient(int retryCount, SqsAsyncIoClient delegate) {
        this.retryCount = retryCount;
        this.delegate = delegate;
    }

    @Override
    public Single<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request) {
        return retry(() -> delegate.receiveMessage(request));
    }

    @Override
    public Single<DeleteMessageResult> deleteMessage(DeleteMessageRequest request) {
        return retry(() -> delegate.deleteMessage(request));
    }

    @Override
    public Single<SendMessageResult> publishMessage(Message message, String queueUrl, Optional<Duration> delay) {
        return retry(() -> delegate.publishMessage(message, queueUrl, delay));
    }

    @Override
    public Single<ChangeMessageVisibilityResult> changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        return retry(() -> delegate.changeMessageVisibility(request));
    }

    @Override
    public Single<DeleteMessageBatchResult> deleteMessageBatch(DeleteMessageBatchRequest request) {
        return retry(() -> delegate.deleteMessageBatch(request));
    }

    @Override
    public Single<SendMessageBatchResult> sendMessageBatch(SendMessageBatchRequest request) {
        return retry(() -> delegate.sendMessageBatch(request));
    }

    @Override
    public Single<ChangeMessageVisibilityBatchResult>
            changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        return retry(() -> delegate.changeMessageVisibilityBatch(request));
    }

    private <S> Single<S> retry(Supplier<Single<S>> func) {
        return Single.defer(func::get).retry(retryCount).subscribeWith(SingleSubject.create());
    }
}
