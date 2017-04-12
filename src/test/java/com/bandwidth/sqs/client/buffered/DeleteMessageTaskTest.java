package com.bandwidth.sqs.client.buffered;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageResult;

import org.junit.Test;

import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class DeleteMessageTaskTest {
    private static final String QUEUE_URL = "http://some/url.domain";

    private final BufferedSqsAsyncIoClient mockSqsClient = mock(BufferedSqsAsyncIoClient.class);
    private final BatchRequest batchRequest = new BatchRequest(QUEUE_URL, null);

    @Test
    public void getMessageResult() {
        assertThat(DeleteMessageTask.getMessageResult.apply(null))
                .isInstanceOf(DeleteMessageResult.class)
                .isNotNull();
    }

    @Test
    public void sendBatchRequestSuccessTest() {
        when(mockSqsClient.deleteMessageBatch(any())).thenReturn(Single.just(new DeleteMessageBatchResult()));

        new DeleteMessageTask.SendBatchRequestTask(mockSqsClient)
                .apply(batchRequest)
                .test().assertComplete();
    }

    @Test
    public void sendBatchRequestErrorTest() {
        Exception exception = new RuntimeException();
        when(mockSqsClient.deleteMessageBatch(any())).thenReturn(Single.error(exception));

        new DeleteMessageTask.SendBatchRequestTask(mockSqsClient).apply(batchRequest)
                .test().assertError(exception);
    }
}
