package com.bandwidth.sqs.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import io.reactivex.Single;

@SuppressWarnings("unchecked")
public class BaseSqsAsyncIoClientTest {

    private static final AWSCredentialsProvider CREDENTIALS_PROVIDER = mock(AWSCredentialsProvider.class);
    private static final String QUEUE_URL = "http://domain.com/account_id/queue_name";
    private static final String RECEIPT_HANDLE = "98h4g_receipt_handle_n7f9";
    private static final String MESSAGE_BODY = "this is a message body";
    private static final int VISIBILITY_TIMEOUT = 17;
    private static final String ENTRY_ID = "1";

    private final AsyncHttpClient asyncHttpClientMock = mock(AsyncHttpClient.class);
    private final AsyncResponseConverter mockResponseAdapter = mock(AsyncResponseConverter.class);
    private final HttpResponse httpResponseMock = mock(HttpResponse.class);
    private final SendMessageResult sendMessageResultMock = mock(SendMessageResult.class);

    private final BaseSqsAsyncIoClient sqsClient = new BaseSqsAsyncIoClient();

    private final AsyncRequestSender requestSenderMock = mock(AsyncRequestSender.class);

    @Before
    public void init() {
        when(httpResponseMock.getStatusCode()).thenReturn(200);

        when(asyncHttpClientMock.executeRequest(any(Request.class), any())).then(invocationOnMock -> {
            ((AsyncCompletionHandler)invocationOnMock.getArgument(1)).onCompleted(mock(Response.class));
            return null;
        });
        when(mockResponseAdapter.apply(any(), any())).thenReturn(httpResponseMock);

        when(requestSenderMock.send(any())).thenReturn(Single.just(sendMessageResultMock));

        sqsClient.setSendMessageRequestSender(requestSenderMock);
        sqsClient.setReceiveMessageRequestSender(requestSenderMock);
        sqsClient.setDeleteMessageRequestSender(requestSenderMock);
        sqsClient.setSendMessageBatchRequestSender(requestSenderMock);
        sqsClient.setDeleteMessageBatchRequestSender(requestSenderMock);
        sqsClient.setChangeMessageVisibilityRequestSender(requestSenderMock);
        sqsClient.setChangeMessageVisibilityBatchRequestSender(requestSenderMock);
    }

    @Test
    public void testReceiveMessage() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(QUEUE_URL)
                .withMaxNumberOfMessages(10);

        sqsClient.receiveMessage(receiveMessageRequest).test().assertComplete();
    }

    @Test
    public void testDeleteMessage() throws InterruptedException {
        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest()
                .withQueueUrl(QUEUE_URL)
                .withReceiptHandle(RECEIPT_HANDLE);

        sqsClient.deleteMessage(deleteMessageRequest).test().assertComplete();
    }

    @Test
    public void testDeleteBatchMessage() throws InterruptedException {
        DeleteMessageBatchRequestEntry entry = new DeleteMessageBatchRequestEntry()
                .withId(ENTRY_ID)
                .withReceiptHandle(RECEIPT_HANDLE);
        DeleteMessageBatchRequest deleteMessageRequest = new DeleteMessageBatchRequest()
                .withQueueUrl(QUEUE_URL)
                .withEntries(entry);
        sqsClient.deleteMessageBatch(deleteMessageRequest).test().assertComplete();
    }

    @Test
    public void testSendMessage() throws InterruptedException {
        sqsClient.publishMessage(new Message(), QUEUE_URL).test().assertComplete();
    }

    @Test
    public void testSendBatchMessage() throws InterruptedException {
        SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry()
                .withId(ENTRY_ID)
                .withMessageBody(MESSAGE_BODY);
        SendMessageBatchRequest sendMessageRequest = new SendMessageBatchRequest()
                .withQueueUrl(QUEUE_URL)
                .withEntries(entry);

        sqsClient.sendMessageBatch(sendMessageRequest).test().assertComplete();
    }

    @Test
    public void testChangeMessageVisibility() {
        ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest()
                .withQueueUrl(QUEUE_URL)
                .withReceiptHandle(RECEIPT_HANDLE)
                .withVisibilityTimeout(VISIBILITY_TIMEOUT);
        sqsClient.changeMessageVisibility(request).test().assertComplete();
    }

    @Test
    public void testChangeMessageBatchVisibility() {
        ChangeMessageVisibilityBatchRequestEntry entry = new ChangeMessageVisibilityBatchRequestEntry()
                .withId(ENTRY_ID)
                .withReceiptHandle(RECEIPT_HANDLE)
                .withVisibilityTimeout(VISIBILITY_TIMEOUT);

        ChangeMessageVisibilityBatchRequest request = new ChangeMessageVisibilityBatchRequest()
                .withQueueUrl(QUEUE_URL)
                .withEntries(entry);
        sqsClient.changeMessageVisibilityBatch(request).test().assertComplete();
    }

    @Test
    public void testSetAwsCredentialsProvider() {
        sqsClient.setAwsCredentialsProvider(CREDENTIALS_PROVIDER);
        assertThat(sqsClient.getAwsCredentialsProvider()).isEqualTo(CREDENTIALS_PROVIDER);
    }

    private <T extends AsyncRequestSender> T mockSender(T sender) {

        return sender;
    }
}
