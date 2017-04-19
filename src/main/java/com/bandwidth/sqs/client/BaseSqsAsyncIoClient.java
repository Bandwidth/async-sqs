package com.bandwidth.sqs.client;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.StaxResponseHandler;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.transform.ChangeMessageVisibilityBatchRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.ChangeMessageVisibilityBatchResultStaxUnmarshaller;
import com.amazonaws.services.sqs.model.transform.ChangeMessageVisibilityRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.ChangeMessageVisibilityResultStaxUnmarshaller;
import com.amazonaws.services.sqs.model.transform.DeleteMessageBatchRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.DeleteMessageBatchResultStaxUnmarshaller;
import com.amazonaws.services.sqs.model.transform.DeleteMessageRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.DeleteMessageResultStaxUnmarshaller;
import com.amazonaws.services.sqs.model.transform.ReceiveMessageRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.ReceiveMessageResultStaxUnmarshaller;
import com.amazonaws.services.sqs.model.transform.SendMessageBatchRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.SendMessageBatchResultStaxUnmarshaller;
import com.amazonaws.services.sqs.model.transform.SendMessageRequestMarshaller;
import com.amazonaws.services.sqs.model.transform.SendMessageResultStaxUnmarshaller;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.reactivex.Single;


public class BaseSqsAsyncIoClient implements SqsAsyncIoClient {

    public static final String FULL_SERVICE_NAME = "AmazonSQS";
    private static final int DEFAULT_CONNECTION_TTL = 10000;

    private AWSCredentialsProvider credentialsProvider;
    private AsyncRequestSender<ReceiveMessageRequest, ReceiveMessageResult> receiveMessageRequestSender;
    private AsyncRequestSender<DeleteMessageRequest, DeleteMessageResult> deleteMessageRequestSender;
    private AsyncRequestSender<SendMessageRequest, SendMessageResult> sendMessageRequestSender;
    private AsyncRequestSender<DeleteMessageBatchRequest, DeleteMessageBatchResult> deleteMessageBatchRequestSender;
    private AsyncRequestSender<SendMessageBatchRequest, SendMessageBatchResult> sendMessageBatchRequestSender;
    private AsyncRequestSender<ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult>
            changeMessageVisibilityRequestSender;
    private AsyncRequestSender<ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult>
            changeMessageVisibilityBatchRequestSender;

    /**
     * Creates a new SQS client with default values.
     */
    public BaseSqsAsyncIoClient() {
        this(new DefaultAsyncHttpClient(
                new DefaultAsyncHttpClientConfig.Builder()
                        .setKeepAlive(true)
                        .setConnectionTtl(DEFAULT_CONNECTION_TTL)
                        .setMaxConnections(Integer.MAX_VALUE)
                        .setMaxConnectionsPerHost(Integer.MAX_VALUE)
                        .build()
        ));
    }

    public BaseSqsAsyncIoClient(AsyncHttpClient asyncHttpClient) {
        this(asyncHttpClient, new AsyncRequestConverter(), new AsyncResponseConverter());
    }

    public BaseSqsAsyncIoClient(AsyncHttpClient asyncHttpClient, Function<SignableRequest<?>, Request>
            requestAdapter,
            BiFunction<Response, com.amazonaws.Request, HttpResponse> responseAdapter) {

        AWSCredentialsProviderChain credentials = new DefaultAWSCredentialsProviderChain();
        AWS4Signer requestSigner = new AWS4Signer();

        setReceiveMessageRequestSender(new AsyncRequestSender<>(
                credentials, requestAdapter, responseAdapter, asyncHttpClient, requestSigner,
                new ReceiveMessageRequestMarshaller(),
                new StaxResponseHandler<>(new ReceiveMessageResultStaxUnmarshaller()),
                ReceiveMessageRequest::getQueueUrl
        ));

        setDeleteMessageRequestSender(new AsyncRequestSender<>(
                credentials, requestAdapter, responseAdapter, asyncHttpClient, requestSigner,
                new DeleteMessageRequestMarshaller(),
                new StaxResponseHandler<>(new DeleteMessageResultStaxUnmarshaller()),
                DeleteMessageRequest::getQueueUrl
        ));

        setSendMessageRequestSender(new AsyncRequestSender<>(
                credentials, requestAdapter, responseAdapter, asyncHttpClient, requestSigner,
                new SendMessageRequestMarshaller(),
                new StaxResponseHandler<>(new SendMessageResultStaxUnmarshaller()),
                SendMessageRequest::getQueueUrl
        ));

        setDeleteMessageBatchRequestSender(new AsyncRequestSender<>(
                credentials, requestAdapter, responseAdapter, asyncHttpClient, requestSigner,
                new DeleteMessageBatchRequestMarshaller(),
                new StaxResponseHandler<>(new DeleteMessageBatchResultStaxUnmarshaller()),
                DeleteMessageBatchRequest::getQueueUrl
        ));

        setSendMessageBatchRequestSender(new AsyncRequestSender<>(
                credentials, requestAdapter, responseAdapter, asyncHttpClient, requestSigner,
                new SendMessageBatchRequestMarshaller(),
                new StaxResponseHandler<>(new SendMessageBatchResultStaxUnmarshaller()),
                SendMessageBatchRequest::getQueueUrl
        ));

        setChangeMessageVisibilityRequestSender(new AsyncRequestSender<>(
                credentials, requestAdapter, responseAdapter, asyncHttpClient, requestSigner,
                new ChangeMessageVisibilityRequestMarshaller(),
                new StaxResponseHandler<>(new ChangeMessageVisibilityResultStaxUnmarshaller()),
                ChangeMessageVisibilityRequest::getQueueUrl
        ));

        setChangeMessageVisibilityBatchRequestSender(new AsyncRequestSender<>(
                credentials, requestAdapter, responseAdapter, asyncHttpClient, requestSigner,
                new ChangeMessageVisibilityBatchRequestMarshaller(),
                new StaxResponseHandler<>(new ChangeMessageVisibilityBatchResultStaxUnmarshaller()),
                ChangeMessageVisibilityBatchRequest::getQueueUrl
        ));
    }

    /**
     * Sets a new AWSCredentialsProvider
     */
    public BaseSqsAsyncIoClient setAwsCredentialsProvider(AWSCredentialsProvider provider) {
        this.credentialsProvider = provider;
        return this;
    }

    public AWSCredentialsProvider getAwsCredentialsProvider() {
        return this.credentialsProvider;
    }


    /**
     * Immediately sends a request to retrieve messages.
     */
    @Override
    public Single<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request) {
        return receiveMessageRequestSender.send(request);
    }

    /**
     * Deletes a single message immediately.
     */
    @Override
    public Single<DeleteMessageResult> deleteMessage(DeleteMessageRequest request) {
        return deleteMessageRequestSender.send(request);
    }

    /**
     * Publishes a message immediately
     *
     * @param message The message to publish
     * @param queueUrl The queue to send the message to
     * @param maybeDelay Amount of time a message is delayed before it can be consumed (Max 15 minutes)
     *                   or the default delay of the SQS queue if "empty"
     * @return
     */
    @Override
    public Single<SendMessageResult> publishMessage(Message message, String queueUrl, Optional<Duration> maybeDelay) {
            SendMessageRequest sendRequest = new SendMessageRequest()
                    .withMessageAttributes(message.getMessageAttributes())
                    .withMessageBody(message.getBody())
                    .withQueueUrl(queueUrl);
            maybeDelay.ifPresent((delay) -> sendRequest.withDelaySeconds((int)delay.getSeconds()));
            return sendMessageRequestSender.send(sendRequest);
    }

    @Override
    public Single<ChangeMessageVisibilityResult> changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        return changeMessageVisibilityRequestSender.send(request);
    }

    @Override
    public Single<DeleteMessageBatchResult> deleteMessageBatch(DeleteMessageBatchRequest request) {
        return deleteMessageBatchRequestSender.send(request);
    }

    @Override
    public Single<SendMessageBatchResult> sendMessageBatch(SendMessageBatchRequest request) {
        return sendMessageBatchRequestSender.send(request);
    }

    @Override
    public Single<ChangeMessageVisibilityBatchResult>
            changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        return changeMessageVisibilityBatchRequestSender.send(request);
    }

    public void setReceiveMessageRequestSender(AsyncRequestSender<ReceiveMessageRequest, ReceiveMessageResult> sender) {
        this.receiveMessageRequestSender = sender;
    }

    public void setDeleteMessageRequestSender(AsyncRequestSender<DeleteMessageRequest, DeleteMessageResult> sender) {
        this.deleteMessageRequestSender = sender;
    }

    public void setSendMessageRequestSender(AsyncRequestSender<SendMessageRequest, SendMessageResult> sender) {
        this.sendMessageRequestSender = sender;
    }

    public void setDeleteMessageBatchRequestSender(AsyncRequestSender<DeleteMessageBatchRequest,
            DeleteMessageBatchResult> sender) {
        this.deleteMessageBatchRequestSender = sender;
    }

    public void setSendMessageBatchRequestSender(AsyncRequestSender<SendMessageBatchRequest, SendMessageBatchResult>
            sender) {
        this.sendMessageBatchRequestSender = sender;
    }

    public void setChangeMessageVisibilityRequestSender(AsyncRequestSender<ChangeMessageVisibilityRequest,
            ChangeMessageVisibilityResult>
            sender) {
        this.changeMessageVisibilityRequestSender = sender;
    }

    public void setChangeMessageVisibilityBatchRequestSender(AsyncRequestSender<ChangeMessageVisibilityBatchRequest,
            ChangeMessageVisibilityBatchResult>
            sender) {
        this.changeMessageVisibilityBatchRequestSender = sender;
    }
}