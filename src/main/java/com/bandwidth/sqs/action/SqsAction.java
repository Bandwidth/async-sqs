package com.bandwidth.sqs.action;

import com.amazonaws.auth.AWSCredentials;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

public interface SqsAction<T> {
    Request toHttpRequest(AWSCredentials credentials);

    T parseHttpResponse(Response httpResponse) throws Exception;

    /**
     * Indicates if this action is a batch action. It is possible for the entire "batch action" to succeed,
     * but have failures of the individual actions in the batch, so retry logic must be processed differently
     * for batch actions
     *
     * @return True if this is a batch action, false otherwise
     */
    boolean isBatchAction();
}
