package com.bandwidth.sqs.actions.sender;

import com.bandwidth.sqs.actions.SqsAction;

import io.reactivex.Single;

public interface SqsRequestSender {
    <T> Single<T> sendRequest(SqsAction<T> request);
}
