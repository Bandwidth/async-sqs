package com.bandwidth.sqs.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.AmazonSQSException;

import org.junit.Test;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.Completable;
import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class RetryingSqsQueueTest {
    private static final int RETRY_COUNT = 2;
    private static final int MAX_MESSAGES = 10;
    private static final String SQS_MESSAGE_ID = "sqs-372o37i4273";
    private static final String RECEIPT_HANDLE = "handle-3rwiugnae";
    private static final String MESSAGE_BODY = "message body";
    private static final Optional<Duration> WAIT_TIME = Optional.empty();
    private static final Optional<Duration> TIMEOUT = Optional.empty();
    private static final Exception TEST_EXCEPTION = new RuntimeException("Unit-test exception");
    private static final AmazonSQSException AWS_CLIENT_EXCEPTION = new AmazonSQSException("unit-test exception");
    private static final AmazonSQSException AWS_SERVICE_EXCEPTION = new AmazonSQSException("unit-test exception");

    private final SqsQueue<String> delegateMock = mock(SqsQueue.class);
    private final MutableSqsQueueAttributes attributesMock = mock(MutableSqsQueueAttributes.class);

    private final RetryingSqsQueue retryingQueue = new RetryingSqsQueue(delegateMock, RETRY_COUNT);

    static {
        AWS_CLIENT_EXCEPTION.setErrorType(AmazonServiceException.ErrorType.Client);
        AWS_SERVICE_EXCEPTION.setErrorType(AmazonServiceException.ErrorType.Service);
    }

    @Test
    public void testNoRetryIfSuccess() {
        when(delegateMock.deleteMessage(anyString())).thenReturn(Completable.complete());
        retryingQueue.deleteMessage(RECEIPT_HANDLE);
        verify(delegateMock).deleteMessage(RECEIPT_HANDLE);
    }

    @Test
    public void testRetryIfAlwaysFails() {
        when(delegateMock.deleteMessage(anyString())).thenThrow(TEST_EXCEPTION);
        retryingQueue.deleteMessage(RECEIPT_HANDLE);
        verify(delegateMock, times(RETRY_COUNT + 1)).deleteMessage(RECEIPT_HANDLE);
    }

    @Test
    public void testRetryUntilSuccess() {
        when(delegateMock.deleteMessage(anyString()))
                .thenThrow(TEST_EXCEPTION)
                .thenReturn(Completable.complete());

        retryingQueue.deleteMessage(RECEIPT_HANDLE);
        verify(delegateMock, times(2)).deleteMessage(RECEIPT_HANDLE);
        assertThat(RETRY_COUNT).isGreaterThanOrEqualTo(2);
    }

    @Test
    public void testNoRetryIfAmazonClientException() {
        when(delegateMock.deleteMessage(anyString())).thenThrow(AWS_CLIENT_EXCEPTION);

        retryingQueue.deleteMessage(RECEIPT_HANDLE);
        verify(delegateMock).deleteMessage(RECEIPT_HANDLE);
    }

    @Test
    public void testRetryIfAmazonServiceException() {
        when(delegateMock.deleteMessage(anyString())).thenThrow(AWS_SERVICE_EXCEPTION);

        retryingQueue.deleteMessage(RECEIPT_HANDLE);
        verify(delegateMock, times(RETRY_COUNT + 1)).deleteMessage(RECEIPT_HANDLE);
    }

    @Test
    public void testChangeMessageVisibility() {
        when(delegateMock.changeMessageVisibility(anyString(), any())).thenReturn(Completable.complete());
        retryingQueue.changeMessageVisibility(RECEIPT_HANDLE, Duration.ZERO);
        verify(delegateMock).changeMessageVisibility(anyString(), any());
    }

    @Test
    public void testPublishMessage() {
        when(delegateMock.publishMessage(anyString(), any())).thenReturn(Single.just(SQS_MESSAGE_ID));
        retryingQueue.publishMessage(MESSAGE_BODY, Optional.of(Duration.ZERO));
        verify(delegateMock).publishMessage(anyString(), any());
    }

    @Test
    public void testGetQueueUrl() {
        retryingQueue.getQueueUrl();
        verify(delegateMock).getQueueUrl();
    }

    @Test
    public void testGetAttributes() {
        retryingQueue.getAttributes();
        verify(delegateMock).getAttributes();
    }

    @Test
    public void testReceiveMessages() {
        retryingQueue.receiveMessages(MAX_MESSAGES, WAIT_TIME, TIMEOUT);
        verify(delegateMock).receiveMessages(MAX_MESSAGES, WAIT_TIME, TIMEOUT);
    }

    @Test
    public void testSetAttributes() {
        retryingQueue.setAttributes(attributesMock);
        verify(delegateMock).setAttributes(attributesMock);
    }
}
