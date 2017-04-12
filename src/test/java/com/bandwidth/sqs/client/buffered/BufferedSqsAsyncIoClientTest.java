package com.bandwidth.sqs.client.buffered;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.bandwidth.sqs.client.BaseSqsAsyncIoClient;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.stream.IntStream;

import io.reactivex.Single;


@SuppressWarnings("unchecked")
public class BufferedSqsAsyncIoClientTest {
    private static final Duration MAX_WAIT = Duration.ofMillis(1234);

    private final BaseSqsAsyncIoClient sqsClientMock = mock(BaseSqsAsyncIoClient.class);

    private final SendMessageBatchResult sendMessageBatchResultMock = mock(SendMessageBatchResult.class);
    private final DeleteMessageBatchResult deleteMessageBatchResultMock = mock(DeleteMessageBatchResult.class);
    private final ReceiveMessageResult receiveMessageResultMock = mock(ReceiveMessageResult.class);
    private final ChangeMessageVisibilityBatchResult changeMessageVisibilityBatchResultMock
            = mock(ChangeMessageVisibilityBatchResult.class);

    private final BufferedSqsAsyncIoClient bufferedSqsClient = new BufferedSqsAsyncIoClient(MAX_WAIT, sqsClientMock);

    @Before
    public void setup() {
        when(sqsClientMock.sendMessageBatch(any())).thenReturn(Single.just(sendMessageBatchResultMock));
        when(sqsClientMock.deleteMessageBatch(any())).thenReturn(Single.just(deleteMessageBatchResultMock));
        when(sqsClientMock.receiveMessage(any())).thenReturn(Single.just(receiveMessageResultMock));
        when(sqsClientMock.changeMessageVisibilityBatch(any()))
                .thenReturn(Single.just(changeMessageVisibilityBatchResultMock));
    }

    @Test
    public void shouldSendBatchWhenBufferReachesMaxBufferSizeTest() {
        SendMessageRequest request = mock(SendMessageRequest.class);
        IntStream.range(0, BufferedSqsAsyncIoClient.MAX_BATCH_SIZE - 1)
                .forEach(i -> bufferedSqsClient.sendMessage(request));
        verify(sqsClientMock, never()).sendMessageBatch(any());
        bufferedSqsClient.sendMessage(request);

        ArgumentCaptor<SendMessageBatchRequest> captor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqsClientMock).sendMessageBatch(captor.capture());
        assertThat(captor.getValue().getEntries().size()).isEqualTo(BufferedSqsAsyncIoClient.MAX_BATCH_SIZE);
    }

    @Test
    public void deleteMessageTest() {
        DeleteMessageRequest request = mock(DeleteMessageRequest.class);
        IntStream.range(0, BufferedSqsAsyncIoClient.MAX_BATCH_SIZE - 1)
                .forEach(i -> bufferedSqsClient.deleteMessage(request));
        verify(sqsClientMock, never()).sendMessageBatch(any());
        bufferedSqsClient.deleteMessage(request);

        ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(sqsClientMock).deleteMessageBatch(captor.capture());
        assertThat(captor.getValue().getEntries().size()).isEqualTo(BufferedSqsAsyncIoClient.MAX_BATCH_SIZE);
    }

    @Test
    public void changeMessageVisibilityTest() {
        ChangeMessageVisibilityRequest request = mock(ChangeMessageVisibilityRequest.class);
        IntStream.range(0, BufferedSqsAsyncIoClient.MAX_BATCH_SIZE - 1)
                .forEach(i -> bufferedSqsClient.changeMessageVisibility(request));
        verify(sqsClientMock, never()).changeMessageVisibility(any());
        bufferedSqsClient.changeMessageVisibility(request);

        ArgumentCaptor<ChangeMessageVisibilityBatchRequest> captor
                = ArgumentCaptor.forClass(ChangeMessageVisibilityBatchRequest.class);
        verify(sqsClientMock).changeMessageVisibilityBatch(captor.capture());
        assertThat(captor.getValue().getEntries().size()).isEqualTo(BufferedSqsAsyncIoClient.MAX_BATCH_SIZE);
    }

    @Test
    public void receiveMessageTest() {
        ReceiveMessageRequest request = mock(ReceiveMessageRequest.class);
        bufferedSqsClient.receiveMessage(request).test().assertResult(receiveMessageResultMock);
        verify(sqsClientMock).receiveMessage(request);
    }

    @Test
    public void sendMessageBatchTest() {
        SendMessageBatchRequest request = mock(SendMessageBatchRequest.class);
        bufferedSqsClient.sendMessageBatch(request).test().assertResult(sendMessageBatchResultMock);
        verify(sqsClientMock).sendMessageBatch(request);
    }

    @Test
    public void deleteMessageBatchTest() {
        DeleteMessageBatchRequest request = mock(DeleteMessageBatchRequest.class);
        bufferedSqsClient.deleteMessageBatch(request).test().assertResult(deleteMessageBatchResultMock);
        verify(sqsClientMock).deleteMessageBatch(request);
    }

    @Test
    public void changeMessageVisibilityBatchTest() {
        ChangeMessageVisibilityBatchRequest request = mock(ChangeMessageVisibilityBatchRequest.class);
        bufferedSqsClient.changeMessageVisibilityBatch(request).test()
                .assertResult(changeMessageVisibilityBatchResultMock);
        verify(sqsClientMock).changeMessageVisibilityBatch(request);
    }

}
