package com.bandwidth.sqs.client;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import org.junit.Test;

import java.util.Optional;

import io.reactivex.Single;

public class RetryingSqsAsyncIoClientTest {

    private static final String QUEUE_URL = "";
    private static final int RETRY_COUNT = 1;
    private static final Exception EXCEPTION = new RuntimeException();

    private final SqsAsyncIoClient coreSqsClientMock = mock(SqsAsyncIoClient.class);

    private final RetryingSqsAsyncIoClient sqsClient = new RetryingSqsAsyncIoClient(RETRY_COUNT, coreSqsClientMock);

    @Test
    public void testNoRetryIfSuccess() {
        when(coreSqsClientMock.publishMessage(any(), any(), any())).thenReturn(Single.just(new SendMessageResult()));
        sqsClient.publishMessage(new Message(), QUEUE_URL, Optional.empty()).test().assertComplete();
        verify(coreSqsClientMock).publishMessage(any(), any(), any());
    }

    @Test
    public void testSendMessageRetry() {
        when(coreSqsClientMock.publishMessage(any(), any(), any())).thenReturn(Single.error(EXCEPTION));
        sqsClient.publishMessage(new Message(), QUEUE_URL, Optional.empty()).test().assertError(EXCEPTION);
        verify(coreSqsClientMock, times(2)).publishMessage(any(), any(), any());
    }

    @Test
    public void testDeleteMessageRetry() {
        when(coreSqsClientMock.deleteMessage(any())).thenReturn(Single.error(EXCEPTION));
        sqsClient.deleteMessage(new DeleteMessageRequest()).test().assertError(EXCEPTION);
        verify(coreSqsClientMock, times(2)).deleteMessage(any());
    }

    @Test
    public void testReceiveMessageRetry() {
        when(coreSqsClientMock.receiveMessage(any())).thenReturn(Single.error(EXCEPTION));
        sqsClient.receiveMessage(new ReceiveMessageRequest()).test().assertError(EXCEPTION);
        verify(coreSqsClientMock, times(2)).receiveMessage(any());
    }

    @Test
    public void testSendMessageBatchRetry() {
        when(coreSqsClientMock.sendMessageBatch(any())).thenReturn(Single.error(EXCEPTION));
        sqsClient.sendMessageBatch(new SendMessageBatchRequest()).test().assertError(EXCEPTION);
        verify(coreSqsClientMock, times(2)).sendMessageBatch(any());
    }

    @Test
    public void testDeleteMessageBatchRetry() {
        when(coreSqsClientMock.deleteMessageBatch(any())).thenReturn(Single.error(EXCEPTION));
        sqsClient.deleteMessageBatch(new DeleteMessageBatchRequest()).test().assertError(EXCEPTION);
        verify(coreSqsClientMock, times(2)).deleteMessageBatch(any());
    }

    @Test
    public void testChangeMessageVisibilityRetry() {
        when(coreSqsClientMock.changeMessageVisibility(any())).thenReturn(Single.error(EXCEPTION));
        sqsClient.changeMessageVisibility(new ChangeMessageVisibilityRequest()).test().assertError(EXCEPTION);
        verify(coreSqsClientMock, times(2)).changeMessageVisibility(any());
    }

    @Test
    public void testChangeMessageVisibilityBatchRetry() {
        when(coreSqsClientMock.changeMessageVisibilityBatch(any())).thenReturn(Single.error(EXCEPTION));
        sqsClient.changeMessageVisibilityBatch(new ChangeMessageVisibilityBatchRequest()).test().assertError(EXCEPTION);
        verify(coreSqsClientMock, times(2)).changeMessageVisibilityBatch(any());
    }

}
