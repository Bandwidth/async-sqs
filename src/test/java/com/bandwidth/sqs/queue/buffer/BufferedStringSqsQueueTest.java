package com.bandwidth.sqs.queue.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.bandwidth.sqs.action.GetQueueAttributesAction;
import com.bandwidth.sqs.action.ReceiveMessagesAction;
import com.bandwidth.sqs.queue.SqsMessage;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueAttributes;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.entry.ChangeMessageVisibilityEntry;
import com.bandwidth.sqs.queue.entry.DeleteMessageEntry;
import com.bandwidth.sqs.queue.entry.SendMessageEntry;
import com.bandwidth.sqs.action.sender.SqsRequestSender;

import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class BufferedStringSqsQueueTest {
    private static final String QUEUE_URL = "https://domain.com/path";
    private static final String MESSAGE_ID = "message-id";
    private static final String MESSAGE_BODY = "message-body";
    private static final String RECEIPT_HANDLE = "receipt-handle";
    private static final SqsQueueClientConfig CLIENT_CONFIG = SqsQueueClientConfig.builder().build();
    private static final SqsQueueAttributes ATTRIBUTES = SqsQueueAttributes.builder().build();
    private static final Message SQS_MESSAGE = new Message()
            .withMessageId(MESSAGE_ID)
            .withBody(MESSAGE_BODY)
            .withReceiptHandle(RECEIPT_HANDLE);

    private final SqsRequestSender requestSenderMock = mock(SqsRequestSender.class);
    private KeyedTaskBuffer<String, SendMessageEntry> sendMessageTaskBufferMock = mock(KeyedTaskBuffer.class);
    private KeyedTaskBuffer<String, DeleteMessageEntry> deleteMessageTaskBufferMock = mock(KeyedTaskBuffer.class);
    private KeyedTaskBuffer<String, ChangeMessageVisibilityEntry> changeMessageVisibilityTaskBufferMock =
            mock(KeyedTaskBuffer.class);

    private final BufferedStringSqsQueue queue = new BufferedStringSqsQueue(QUEUE_URL, requestSenderMock,
            CLIENT_CONFIG, Optional.of(ATTRIBUTES));
    private final SqsQueue<String> queueWithoutAttributes = new BufferedStringSqsQueue(QUEUE_URL, requestSenderMock,
            CLIENT_CONFIG, Optional.empty());

    public BufferedStringSqsQueueTest() {
        queue.setSendMessageTaskBuffer(sendMessageTaskBufferMock);
        queue.setDeleteMessageTaskBuffer(deleteMessageTaskBufferMock);
        queue.setChangeMessageVisibilityTaskBuffer(changeMessageVisibilityTaskBufferMock);

        when(requestSenderMock.sendRequest(any(GetQueueAttributesAction.class))).thenReturn(Single.just(
                new GetQueueAttributesResult().withAttributes(ATTRIBUTES.getStringMap())
        ));
        when(requestSenderMock.sendRequest(any(ReceiveMessagesAction.class))).thenReturn(Single.just(
                new ReceiveMessageResult().withMessages(SQS_MESSAGE)
        ));
    }

    @Test
    public void testGetQueueUrl() {
        assertThat(queue.getQueueUrl()).isEqualTo(QUEUE_URL);
    }

    @Test
    public void testGetCachedAttributes() {
        assertThat(queue.getAttributes().blockingGet()).isEqualTo(ATTRIBUTES);
        verifyZeroInteractions(requestSenderMock);//make sure it was actually cached
    }

    @Test
    public void testGetNonCachedAttributes() {
        assertThat(queueWithoutAttributes.getAttributes().blockingGet()).isEqualTo(ATTRIBUTES);
        verify(requestSenderMock).sendRequest(any(GetQueueAttributesAction.class));//it was NOT cached
        assertThat(queueWithoutAttributes.getAttributes().blockingGet()).isEqualTo(ATTRIBUTES);
        verify(requestSenderMock).sendRequest(any(GetQueueAttributesAction.class));//it WAS cached the 2nd time
    }

    @Test
    public void testPublishMessage() {
        queue.publishMessage(MESSAGE_BODY);
        verify(sendMessageTaskBufferMock).addData(eq(QUEUE_URL), any());
    }

    @Test
    public void testDeleteMessage() {
        queue.deleteMessage(RECEIPT_HANDLE);
        verify(deleteMessageTaskBufferMock).addData(eq(QUEUE_URL), any());
    }

    @Test
    public void testChangeMessageVisibility() {
        queue.changeMessageVisibility(RECEIPT_HANDLE, Duration.ZERO);
        verify(changeMessageVisibilityTaskBufferMock).addData(eq(QUEUE_URL), any());
    }

    @Test
    public void testReceiveMessages() {
        List<SqsMessage<String>> messages = queue.receiveMessages().blockingGet();
        assertThat(messages.size()).isEqualTo(1);
        SqsMessage<String> message = messages.get(0);
        assertThat(message.getBody()).isEqualTo(MESSAGE_BODY);
        assertThat(message.getReceiptHandle()).isEqualTo(RECEIPT_HANDLE);
        assertThat(message.getId()).isEqualTo(MESSAGE_ID);
    }
}
