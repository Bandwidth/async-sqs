package com.bandwidth.sqs.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.bandwidth.sqs.actions.CreateQueueAction;
import com.bandwidth.sqs.actions.GetQueueUrlAction;
import com.bandwidth.sqs.actions.SetQueueAttributesAction;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueAttributes;
import com.bandwidth.sqs.queue.SqsQueueConfig;
import com.bandwidth.sqs.request_sender.SqsRequestSender;

import org.junit.Test;

import io.reactivex.Single;

public class DefaultSqsClientTest {
    private static final AmazonSQSException QUEUE_ALREADY_EXISTS_EXCEPTION = new AmazonSQSException("");
    private static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExists";
    private static final String QUEUE_URL = "https://domain.com/12345/queue-name";
    private static final String QUEUE_NAME = "queue-name";
    private static final SqsQueueAttributes ATTRIBUTES = SqsQueueAttributes.builder().build();
    private static final SqsQueueConfig QUEUE_CONFIG = SqsQueueConfig.builder()
            .name(QUEUE_NAME)
            .attributes(ATTRIBUTES)
            .region(Regions.US_EAST_1)
            .build();


    private final SqsRequestSender requestSenderMock = mock(SqsRequestSender.class);

    private final DefaultSqsClient client = new DefaultSqsClient(requestSenderMock);

    public DefaultSqsClientTest() {
        QUEUE_ALREADY_EXISTS_EXCEPTION.setErrorCode(QUEUE_ALREADY_EXISTS);

        when(requestSenderMock.sendRequest(any(GetQueueUrlAction.class)))
                .thenReturn(Single.just(new GetQueueUrlResult().withQueueUrl(QUEUE_URL)));
        when(requestSenderMock.sendRequest(any(SetQueueAttributesAction.class)))
                .thenReturn(Single.just(new SetQueueAttributesResult()));
        when(requestSenderMock.sendRequest(any(GetQueueUrlAction.class)))
                .thenReturn(Single.just(new GetQueueUrlResult().withQueueUrl(QUEUE_URL)));
    }

    @Test
    public void testGetQueueFromUrl() {
        SqsQueue<String> queue = client.getQueueFromUrl(QUEUE_URL);
        assertThat(queue.getQueueUrl()).isEqualTo(QUEUE_URL);
        verifyZeroInteractions(requestSenderMock);
    }

    @Test
    public void testGetQueueFromName() {
        SqsQueue<String> queue = client.getQueueFromName(QUEUE_NAME, Regions.US_EAST_1).blockingGet();
        assertThat(queue.getQueueUrl()).isEqualTo(QUEUE_URL);
        verify(requestSenderMock).sendRequest(any(GetQueueUrlAction.class));
    }

    @Test
    public void testAssertQueueAlreadyExistsAndMatches() {
        when(requestSenderMock.sendRequest(any(CreateQueueAction.class))).thenReturn(Single.just(
                new CreateQueueResult().withQueueUrl(QUEUE_URL)
        ));

        SqsQueue<String> queue = client.assertQueue(QUEUE_CONFIG).blockingGet();
        assertThat(queue.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(queue.getAttributes().blockingGet()).isEqualTo(ATTRIBUTES);
        verify(requestSenderMock).sendRequest(any(CreateQueueAction.class));
        verifyNoMoreInteractions(requestSenderMock);//make sure getAttributes() was cached
    }

    @Test
    public void testAssertQueueAlreadyExistsWrongAttributes() {
        when(requestSenderMock.sendRequest(any(CreateQueueAction.class))).thenReturn(Single.error(
                QUEUE_ALREADY_EXISTS_EXCEPTION
        ));

        SqsQueue<String> queue = client.assertQueue(QUEUE_CONFIG).blockingGet();
        assertThat(queue.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(queue.getAttributes().blockingGet()).isEqualTo(ATTRIBUTES);
        verify(requestSenderMock).sendRequest(any(CreateQueueAction.class));
        verify(requestSenderMock).sendRequest(any(GetQueueUrlAction.class));
        verify(requestSenderMock).sendRequest(any(SetQueueAttributesAction.class));
        verifyNoMoreInteractions(requestSenderMock);//make sure getAttributes() was cached
    }

    @Test
    public void testAssertQueueUnknownError() {
        when(requestSenderMock.sendRequest(any(CreateQueueAction.class))).thenReturn(Single.error(
                new RuntimeException("Unknown error")
        ));

        client.assertQueue(QUEUE_CONFIG).test().assertError(RuntimeException.class);
        verify(requestSenderMock).sendRequest(any(CreateQueueAction.class));
    }

    @Test
    public void testAssertQueueUnknownAmazonError() {
        when(requestSenderMock.sendRequest(any(CreateQueueAction.class))).thenReturn(Single.error(
                new AmazonSQSException("Unknown error")
        ));

        client.assertQueue(QUEUE_CONFIG).test().assertError(RuntimeException.class);
        verify(requestSenderMock).sendRequest(any(CreateQueueAction.class));
    }
}
