package com.bandwidth.sqs.action.sender;

import com.bandwidth.sqs.action.SqsAction;

import io.reactivex.Single;

public interface SqsRequestSender {
    <T> Single<T> sendRequest(SqsAction<T> request);
}
