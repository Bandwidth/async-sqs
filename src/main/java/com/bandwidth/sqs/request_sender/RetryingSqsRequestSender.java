package com.bandwidth.sqs.request_sender;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.bandwidth.sqs.actions.SqsAction;

import java.util.Objects;

import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.BiPredicate;
import io.reactivex.subjects.SingleSubject;

public class RetryingSqsRequestSender implements SqsRequestSender {

    private final int retryCount;
    private final SqsRequestSender delegate;

    public RetryingSqsRequestSender(int retryCount, SqsRequestSender delegate) {
        this.retryCount = retryCount;
        this.delegate = delegate;
    }

    @Override
    public <T> Single<T> sendRequest(SqsAction<T> request) {
        return Single.defer(() -> delegate.sendRequest(request))
                .retry((errCount, error) -> {
                    if (errCount > retryCount) {
                        return false;
                    }
                    if (error instanceof AmazonSQSException) {
                        return ((AmazonSQSException) error).getErrorType() == AmazonServiceException.ErrorType.Service;
                    }
                    return true;
                }).subscribeWith(SingleSubject.create());//convert to Hot single
    }
}
