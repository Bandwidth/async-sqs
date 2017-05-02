package com.bandwidth.sqs.queue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

@SuppressWarnings("unchecked")
public class MappingSqsQueueTest {

    private static final String RECEIPT_HANDLE = "receipt-handle";
    private static final String MESSAGE_ID = "message-id";
    private static final SqsQueueAttributes ATTRIBUTES = SqsQueueAttributes.builder().build();
    private static final Integer DESERIALIZED_VALUE = 42;
    private static final String SERIALIZED_VALUE = "42";
    private static final RuntimeException SERIALIZATION_ERR = new RuntimeException("serialization error");
    private static final SqsMessage<String> STRING_MESSAGE = SqsMessage.<String>builder()
            .body(SERIALIZED_VALUE)
            .id(MESSAGE_ID)
            .receiptHandle(RECEIPT_HANDLE)
            .build();

    private final Function<String, Integer> deserialize = mock(Function.class);
    private final Function<Integer, String> serialize = mock(Function.class);
    private final SqsQueue<String> delegateMock = mock(SqsQueue.class);
    private final SqsQueue<Integer> sqsQueue = new MappingSqsQueue<>(delegateMock, deserialize, serialize);

    public MappingSqsQueueTest() throws Exception {
        when(deserialize.apply(any())).thenReturn(DESERIALIZED_VALUE);
        when(serialize.apply(anyInt())).thenReturn(SERIALIZED_VALUE);

        when(delegateMock.publishMessage(any(), any())).thenReturn(Single.just(MESSAGE_ID));
        when(delegateMock.deleteMessage(any(String.class))).thenReturn(Completable.complete());
        when(delegateMock.receiveMessages(anyInt(), any(), any(Optional.class)))
                .thenReturn(Single.just(Collections.singletonList(STRING_MESSAGE)));
    }

    @Test
    public void testGetQueueUrl() {
        sqsQueue.getQueueUrl();
        verify(delegateMock).getQueueUrl();
    }

    @Test
    public void testGetAttributes() {
        sqsQueue.getAttributes();
        verify(delegateMock).getAttributes();
    }

    @Test
    public void testDeleteMessage() {
        sqsQueue.deleteMessage(RECEIPT_HANDLE).test().assertComplete();
        verify(delegateMock).deleteMessage(RECEIPT_HANDLE);
    }

    @Test
    public void testChangeMessageVisibility() {
        sqsQueue.changeMessageVisibility(RECEIPT_HANDLE, Duration.ZERO);
        verify(delegateMock).changeMessageVisibility(RECEIPT_HANDLE, Duration.ZERO);
    }

    @Test
    public void testSetAttributes() {
        sqsQueue.setAttributes(ATTRIBUTES);
        verify(delegateMock).setAttributes(ATTRIBUTES);
    }

    @Test
    public void testPublishMessageSuccess() {
        sqsQueue.publishMessage(DESERIALIZED_VALUE, Optional.empty()).test().assertComplete();
        verify(delegateMock).publishMessage(SERIALIZED_VALUE, Optional.empty());
    }

    @Test
    public void testPublishMessageError() throws Exception {
        when(serialize.apply(anyInt())).thenThrow(SERIALIZATION_ERR);
        sqsQueue.publishMessage(DESERIALIZED_VALUE).test().assertError(SERIALIZATION_ERR);
    }

    @Test
    public void testReceiveMessagesSuccess() {
        sqsQueue.receiveMessages().test().assertComplete();
        verify(delegateMock).receiveMessages(anyInt(), any(), any(Optional.class));
    }

    @Test
    public void testReceiveMessagesError() throws Exception {
        when(deserialize.apply(any())).thenThrow(SERIALIZATION_ERR);
        sqsQueue.receiveMessages().test().assertError(SERIALIZATION_ERR);
    }
}
