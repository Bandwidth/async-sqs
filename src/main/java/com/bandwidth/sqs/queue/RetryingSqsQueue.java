package com.bandwidth.sqs.queue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.AmazonSQSException;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.subjects.CompletableSubject;
import io.reactivex.subjects.SingleSubject;

/**
 * The SqsRequestSender is only able to retry non-batched actions, since it doesn't have access to the individual
 * actions inside of a batch. This class retries each individual action.
 * The following actions are batched/retried here, the rest are not
 *  - SendMessage
 *  - ChangeMessageVisibility
 *  - DeleteMessage
 */
public class RetryingSqsQueue<T> implements SqsQueue<T> {

    private final SqsQueue<T> delegate;
    private final int retryCount;

    public RetryingSqsQueue(SqsQueue<T> delegate, int retryCount) {
        this.delegate = delegate;
        this.retryCount = retryCount;
    }


    @Override
    public Completable deleteMessage(String receiptHandle) {
        return Completable.defer(() -> delegate.deleteMessage(receiptHandle))
                .retry(this::shouldRetry)
                .subscribeWith(CompletableSubject.create());//convert to Hot completable
    }

    @Override
    public Completable changeMessageVisibility(String receiptHandle, Duration newVisibility) {
        return Completable.defer(() -> delegate.changeMessageVisibility(receiptHandle, newVisibility))
                .retry(this::shouldRetry)
                .subscribeWith(CompletableSubject.create());//convert to Hot completable
    }

    @Override
    public Single<String> publishMessage(T body, Optional<Duration> maybeDelay) {
        return Single.defer(() -> delegate.publishMessage(body, maybeDelay))
                .retry(this::shouldRetry)
                .subscribeWith(SingleSubject.create());//convert to Hot single
    }


    private boolean shouldRetry(int errCount, Throwable error) {
        if (errCount > retryCount) {
            return false;
        }
        if (error instanceof AmazonSQSException) {
            return ((AmazonSQSException) error).getErrorType() != AmazonServiceException.ErrorType.Client;
        }
        return true;
    }

    @Override
    public String getQueueUrl() {
        return delegate.getQueueUrl();
    }

    @Override
    public Single<SqsQueueAttributes> getAttributes() {
        return delegate.getAttributes();
    }

    @Override
    public Single<List<SqsMessage<T>>> receiveMessages(int maxMessages, Optional<Duration> waitTime,
            Optional<Duration> visibilityTimeout) {
        return delegate.receiveMessages(maxMessages, waitTime, visibilityTimeout);
    }

    @Override
    public Completable setAttributes(MutableSqsQueueAttributes attributes) {
        return delegate.setAttributes(attributes);
    }


}
