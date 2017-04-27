package com.bandwidth.sqs.actions;

import com.amazonaws.auth.AWSCredentials;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

public interface SqsAction<T> {
    Request toHttpRequest(AWSCredentials credentials);

    T parseHttpResponse(Response httpResponse) throws Exception;


}
