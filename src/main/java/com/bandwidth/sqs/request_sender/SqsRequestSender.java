package com.bandwidth.sqs.request_sender;

import com.bandwidth.sqs.actions.SqsAction;

import io.reactivex.Single;

public interface SqsRequestSender {
    <T> Single<T> sendRequest(SqsAction<T> request);
}
