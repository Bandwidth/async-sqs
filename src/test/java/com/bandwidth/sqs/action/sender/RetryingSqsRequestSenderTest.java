package com.bandwidth.sqs.action.sender;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.bandwidth.sqs.action.SqsAction;

import org.junit.Test;

import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class RetryingSqsRequestSenderTest {
    private static final int RETRY_COUNT = 1;
    private static final Object ACTION_RESPONSE = new Object();
    private static final AmazonSQSException CLIENT_EXCEPTION = new AmazonSQSException("client err");
    private static final AmazonSQSException SERVER_EXCEPTION = new AmazonSQSException("server err");

    private final SqsAction<Object> actionMock = mock(SqsAction.class);
    private final SqsRequestSender delegateMock = mock(SqsRequestSender.class);
    private final RetryingSqsRequestSender requestSender = new RetryingSqsRequestSender(RETRY_COUNT, delegateMock);

    public RetryingSqsRequestSenderTest() {
        CLIENT_EXCEPTION.setErrorType(AmazonServiceException.ErrorType.Client);
        SERVER_EXCEPTION.setErrorType(AmazonServiceException.ErrorType.Service);
    }

    @Test
    public void testFirstSuccess() {
        when(delegateMock.sendRequest(any())).thenReturn(Single.just(ACTION_RESPONSE));
        requestSender.sendRequest(actionMock).test().assertValue(ACTION_RESPONSE);
        verify(delegateMock).sendRequest(any());//only 1 request sent
    }

    @Test
    public void testRetryOnce() {
        when(delegateMock.sendRequest(any()))
                .thenThrow(new RuntimeException())
                .thenReturn(Single.just(ACTION_RESPONSE));
        requestSender.sendRequest(actionMock).test().assertValue(ACTION_RESPONSE);
        verify(delegateMock, times(2)).sendRequest(any());//exactly 2 requests sent
    }

    @Test
    public void testRetryFail() {
        when(delegateMock.sendRequest(any()))
                .thenThrow(new RuntimeException());
        requestSender.sendRequest(actionMock).test().assertError(RuntimeException.class);
        verify(delegateMock, times(2)).sendRequest(any());//exactly 2 requests sent
    }

    @Test
    public void testNoRetryForClientError() {
        when(delegateMock.sendRequest(any()))
                .thenThrow(CLIENT_EXCEPTION);
        requestSender.sendRequest(actionMock).test().assertError(AmazonServiceException.class);
        verify(delegateMock, times(1)).sendRequest(any());//only 1 request sent
    }

    @Test
    public void testRetryForServerError() {
        when(delegateMock.sendRequest(any()))
                .thenThrow(SERVER_EXCEPTION);
        requestSender.sendRequest(actionMock).test().assertError(AmazonSQSException.class);
        verify(delegateMock, times(2)).sendRequest(any());//exactly 2 requests sent
    }

    @Test
    public void testNoRetryIfBatchAction() {
        when(actionMock.isBatchAction()).thenReturn(true);
        when(delegateMock.sendRequest(any()))
                .thenThrow(new RuntimeException());

        requestSender.sendRequest(actionMock).test().assertError(RuntimeException.class);
        verify(delegateMock).sendRequest(any());
    }
}
