package com.bandwidth.sqs.consumer.acknowledger;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger.AckMode;

import com.amazonaws.services.sqs.model.Message;
import com.bandwidth.sqs.queue.SqsQueue;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import io.reactivex.Completable;
import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class MessageAcknowledgerTest {
    private static final String RECEIPT_ID = "123-adfg-w4-dfga-346-zfg";
    private static final String MESSAGE_ID = "message-id-q4tqeeg";
    private static final String MESSAGE = "message body";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final SqsQueue<String> sqsQueueMock = mock(SqsQueue.class);
    private final SqsQueue<Object> sqsObjectQueueMock = mock(SqsQueue.class);
    private final MessageAcknowledger<String> messageAcknowledger =
            new MessageAcknowledger(sqsQueueMock, RECEIPT_ID, Optional.of(Instant.now().plus(TIMEOUT)));

    public MessageAcknowledgerTest() {
        when(sqsQueueMock.deleteMessage(any(String.class))).thenReturn(Completable.complete());
        when(sqsQueueMock.changeMessageVisibility(any(String.class), any())).thenReturn(Completable.complete());
        when(sqsQueueMock.publishMessage(any(), any())).thenReturn(Single.just(MESSAGE_ID));
    }

    @Test
    public void testDelete() {
        messageAcknowledger.delete();
        verify(sqsQueueMock).deleteMessage(any(String.class));
        assertCompletedMode(MessageAcknowledger.AckMode.DELETE);
    }

    @Test
    public void testIgnore() {
        messageAcknowledger.ignore();
        verifyZeroInteractions(sqsQueueMock);
        assertCompletedMode(MessageAcknowledger.AckMode.IGNORE);
    }

    @Test
    public void testRetry() {
        messageAcknowledger.retry();
        verifyZeroInteractions(sqsQueueMock);
        assertCompletedMode(MessageAcknowledger.AckMode.RETRY);
    }

    @Test
    public void testDelay() {
        messageAcknowledger.delay(Duration.ZERO);
        verify(sqsQueueMock).changeMessageVisibility(any(String.class), any());
        assertCompletedMode(MessageAcknowledger.AckMode.DELAY);
    }

    @Test
    public void testModify() {
        messageAcknowledger.replace(MESSAGE);
        verify(sqsQueueMock).publishMessage(any(), any());
        verify(sqsQueueMock).deleteMessage(any(String.class));
        assertCompletedMode(MessageAcknowledger.AckMode.MODIFY);
    }

    @Test
    public void testTransfer() {
        messageAcknowledger.transfer(MESSAGE, sqsQueueMock);
        verify(sqsQueueMock).publishMessage(any(), any());
        verify(sqsQueueMock).deleteMessage(any(String.class));
        assertCompletedMode(MessageAcknowledger.AckMode.TRANSFER);
    }

    @Test
    public void testTimeout() {
        MessageAcknowledger<String> messageAcknowledger =
                new MessageAcknowledger(sqsQueueMock, RECEIPT_ID, Optional.of(Instant.now()));
        messageAcknowledger.getCompletable().blockingAwait();
        assertThat(messageAcknowledger.getAckMode().blockingGet()).isEqualTo(AckMode.IGNORE);
    }

    @Test
    public void testNoTimeout() {
        MessageAcknowledger<String> messageAcknowledger =
                new MessageAcknowledger(sqsQueueMock, RECEIPT_ID, Optional.empty());
        messageAcknowledger.getCompletable().test().assertNotComplete();
        messageAcknowledger.getAckMode().test().assertNotComplete();
    }

    @Test
    public void testSuccessfulAckModes() {
        assertThat(AckMode.DELETE.isSuccessful()).isTrue();
        assertThat(AckMode.TRANSFER.isSuccessful()).isTrue();
        assertThat(AckMode.IGNORE.isSuccessful()).isFalse();
        assertThat(AckMode.RETRY.isSuccessful()).isFalse();
        assertThat(AckMode.MODIFY.isSuccessful()).isFalse();
        assertThat(AckMode.DELAY.isSuccessful()).isFalse();
    }

    @Test
    public void testDelegation() {
        MessageAcknowledger<Object> customAcker = new MessageAcknowledger<>(messageAcknowledger, sqsObjectQueueMock);
        customAcker.ignore();

        customAcker.getAckMode().test().assertValue(MessageAcknowledger.AckMode.IGNORE);
        messageAcknowledger.getAckMode().test().assertValue(MessageAcknowledger.AckMode.IGNORE);

        customAcker.getCompletable().test().assertComplete();
        messageAcknowledger.getCompletable().test().assertComplete();
    }

    private void assertCompletedMode(MessageAcknowledger.AckMode mode) {
        messageAcknowledger.getAckMode().test().assertValue(mode);
        messageAcknowledger.getCompletable().test().assertComplete();
    }
}