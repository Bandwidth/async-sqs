package com.bandwidth.sqs.request_sender;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.bandwidth.sqs.actions.SqsAction;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;

import io.reactivex.Single;
import io.reactivex.subjects.SingleSubject;

public class BaseSqsRequestSender implements SqsRequestSender {
    private final AsyncHttpClient httpClient;
    private AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    public BaseSqsRequestSender(AsyncHttpClient httpClient, AWSCredentialsProvider credentialsProvider) {
        this.httpClient = httpClient;
        this.credentialsProvider = credentialsProvider;
    }

    @Override
    public <T> Single<T> sendRequest(SqsAction<T> action) {
        org.asynchttpclient.Request asyncRequest = action.toHttpRequest(credentialsProvider.getCredentials());
        SingleSubject<T> responseSubject = SingleSubject.create();
        httpClient.executeRequest(asyncRequest, new AsyncCompletionHandler<Response>() {
            @Override
            public Response onCompleted(Response httpResponse) {
                Single.fromCallable(() -> action.parseHttpResponse(httpResponse))
                        .subscribeWith(responseSubject);
                return httpResponse;
            }

            @Override
            public void onThrowable(Throwable throwable) {
                responseSubject.onError(throwable);
            }
        });
        return responseSubject;
    }
}
