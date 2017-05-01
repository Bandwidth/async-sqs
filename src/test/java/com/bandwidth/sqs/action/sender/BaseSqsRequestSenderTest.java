package com.bandwidth.sqs.action.sender;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.bandwidth.sqs.action.SqsAction;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class BaseSqsRequestSenderTest {

    private final AsyncHttpClient asyncHttpClientMock = mock(AsyncHttpClient.class);
    private final AWSCredentialsProvider credentialsProviderMock = mock(AWSCredentialsProvider.class);
    private final SqsAction<Object> actionMock = mock(SqsAction.class);
    private final Response httpResponseMock = mock(Response.class);
    private final Object actionResponse = mock(Object.class);

    private final BaseSqsRequestSender requestSender =
            new BaseSqsRequestSender(asyncHttpClientMock, credentialsProviderMock);

    public BaseSqsRequestSenderTest() throws Exception {
        when(actionMock.parseHttpResponse(any())).thenReturn(actionResponse);
    }

    @Test
    public void testSendRequestSuccess() throws Exception {
        Single<Object> result = requestSender.sendRequest(actionMock);
        result.test().assertNotComplete();
        ArgumentCaptor<AsyncCompletionHandler> captor = ArgumentCaptor.forClass(AsyncCompletionHandler.class);
        verify(asyncHttpClientMock).executeRequest((Request) any(), captor.capture());
        AsyncCompletionHandler handler = captor.getValue();

        handler.onCompleted(httpResponseMock);
        result.test().assertValue(actionResponse);
    }

    @Test
    public void testSendRequestOnThrowable() throws Exception {
        Single<Object> result = requestSender.sendRequest(actionMock);
        result.test().assertNotComplete();
        ArgumentCaptor<AsyncCompletionHandler> captor = ArgumentCaptor.forClass(AsyncCompletionHandler.class);
        verify(asyncHttpClientMock).executeRequest((Request) any(), captor.capture());
        AsyncCompletionHandler handler = captor.getValue();

        RuntimeException exception = new RuntimeException("error");
        handler.onThrowable(exception);
        result.test().assertError(exception);
    }

    @Test
    public void testSendRequestParseFailed() throws Exception {
        RuntimeException exception = new RuntimeException("error");
        when(actionMock.parseHttpResponse(any())).thenThrow(exception);
        Single<Object> result = requestSender.sendRequest(actionMock);
        result.test().assertNotComplete();
        ArgumentCaptor<AsyncCompletionHandler> captor = ArgumentCaptor.forClass(AsyncCompletionHandler.class);
        verify(asyncHttpClientMock).executeRequest((Request) any(), captor.capture());
        AsyncCompletionHandler handler = captor.getValue();
        handler.onCompleted(httpResponseMock);
        result.test().assertError(exception);
    }

}
