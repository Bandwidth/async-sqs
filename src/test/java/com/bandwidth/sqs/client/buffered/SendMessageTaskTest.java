package com.bandwidth.sqs.client.buffered;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageResult;

import org.junit.Test;

import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class SendMessageTaskTest {
    private static final String BATCH_ENTRY_ID = "aw98ha4qo3yy4ahulire";
    private static final String MESSAGE_ID = "msg-id-374gbliuanerr";
    private static final String MESSAGE_ATTRIBUTE_MD5 = "a3o74aliurnf";
    private static final String MESSAGE_BODY_MD5 = "8ap9hu4ifna3y4gfkayu";
    private static final String QUEUE_URL = "http://some/url.domain";

    private final BufferedSqsAsyncIoClient mockSqsClient = mock(BufferedSqsAsyncIoClient.class);
    private final SendMessageTask task = new SendMessageTask(mockSqsClient);
    private final AsyncHandler handler = mock(AsyncHandler.class);
    private final BatchRequest batchRequest = new BatchRequest(QUEUE_URL, null);
    private final SendMessageBatchResult sendMessageBatchResult = mock(SendMessageBatchResult.class);

    @Test
    public void getMessageResultTest() {
        SendMessageBatchResultEntry entry = new SendMessageBatchResultEntry()
                .withMessageId(MESSAGE_ID)
                .withMD5OfMessageAttributes(MESSAGE_ATTRIBUTE_MD5)
                .withMD5OfMessageBody(MESSAGE_BODY_MD5);
        SendMessageResult result = SendMessageTask.getMessageResult.apply(entry);

        assertThat(result.getMessageId()).isEqualTo(MESSAGE_ID);
        assertThat(result.getMD5OfMessageAttributes()).isEqualTo(MESSAGE_ATTRIBUTE_MD5);
        assertThat(result.getMD5OfMessageBody()).isEqualTo(MESSAGE_BODY_MD5);
    }

    @Test
    public void sendBatchRequestSuccessTest() {
        when(mockSqsClient.sendMessageBatch(any())).thenReturn(Single.just(sendMessageBatchResult));

        new SendMessageTask.SendBatchRequestTask(mockSqsClient)
                .apply(batchRequest)
                .test().assertComplete();
    }

    @Test
    public void sendBatchRequestErrorTest() {
        Exception exception = new RuntimeException();

        when(mockSqsClient.sendMessageBatch(any())).thenReturn(Single.error(exception));

        new SendMessageTask.SendBatchRequestTask(mockSqsClient)
                .apply(batchRequest)
                .test().assertError(exception);

    }

}
