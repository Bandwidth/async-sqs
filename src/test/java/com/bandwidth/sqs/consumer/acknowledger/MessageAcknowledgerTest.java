package com.bandwidth.sqs.consumer.acknowledger;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.bandwidth.sqs.client.SqsAsyncIoClient;
import com.bandwidth.sqs.consumer.acknowledger.DefaultMessageAcknowledger;
import com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class MessageAcknowledgerTest {
    private static final String QUEUE_URL = "http://domain.com/path";
    private static final String RECEIPT_ID = "123-adfg-w4-dfga-346-zfg";

    private final SqsAsyncIoClient sqsAsyncIoClientMock = mock(SqsAsyncIoClient.class);
    private final Message message = new Message().withBody("body");

    private final MessageAcknowledger<Message> messageAcknowledger =
            new DefaultMessageAcknowledger(sqsAsyncIoClientMock, QUEUE_URL, RECEIPT_ID);

    @Before
    public void setup() {
        when(sqsAsyncIoClientMock.deleteMessage(any())).thenReturn(Single.just(mock(DeleteMessageResult.class)));
        when(sqsAsyncIoClientMock.changeMessageVisibility(any()))
                .thenReturn(Single.just(mock(ChangeMessageVisibilityResult.class)));
        when(sqsAsyncIoClientMock.sendMessage(any())).thenReturn(Single.just(mock(SendMessageResult.class)));
    }

    @Test
    public void testDelete() {
        messageAcknowledger.delete();
        verify(sqsAsyncIoClientMock).deleteMessage(any());
        assertCompletedMode(MessageAcknowledger.AckMode.DELETE);
    }

    @Test
    public void testIgnore() {
        messageAcknowledger.ignore();
        verifyZeroInteractions(sqsAsyncIoClientMock);
        assertCompletedMode(MessageAcknowledger.AckMode.IGNORE);
    }

    @Test
    public void testRetry() {
        messageAcknowledger.retry();
        verifyZeroInteractions(sqsAsyncIoClientMock);
        assertCompletedMode(MessageAcknowledger.AckMode.RETRY);
    }

    @Test
    public void testDelay() {
        messageAcknowledger.delay(Duration.ZERO);
        verify(sqsAsyncIoClientMock).changeMessageVisibility(any());
        assertCompletedMode(MessageAcknowledger.AckMode.DELAY);
    }

    @Test
    public void testModify() {
        messageAcknowledger.modify(message, Duration.ZERO);
        verify(sqsAsyncIoClientMock).sendMessage(any());
        verify(sqsAsyncIoClientMock).deleteMessage(any());
        assertCompletedMode(MessageAcknowledger.AckMode.MODIFY);
    }

    @Test
    public void testTransfer() {
        messageAcknowledger.transfer(message, QUEUE_URL, Duration.ZERO);
        verify(sqsAsyncIoClientMock).sendMessage(any());
        verify(sqsAsyncIoClientMock).deleteMessage(any());
        assertCompletedMode(MessageAcknowledger.AckMode.TRANSFER);
    }

    @Test
    public void testSuccessfulAckModes() {
        assertThat(MessageAcknowledger.AckMode.DELETE.isSuccessful()).isTrue();
        assertThat(MessageAcknowledger.AckMode.TRANSFER.isSuccessful()).isTrue();
        assertThat(MessageAcknowledger.AckMode.IGNORE.isSuccessful()).isFalse();
        assertThat(MessageAcknowledger.AckMode.RETRY.isSuccessful()).isFalse();
        assertThat(MessageAcknowledger.AckMode.MODIFY.isSuccessful()).isFalse();
        assertThat(MessageAcknowledger.AckMode.DELAY.isSuccessful()).isFalse();
    }

    private void assertCompletedMode(MessageAcknowledger.AckMode mode) {
        messageAcknowledger.getAckMode().test().assertValue(mode);
        messageAcknowledger.getCompletable().test().assertComplete();
    }
}