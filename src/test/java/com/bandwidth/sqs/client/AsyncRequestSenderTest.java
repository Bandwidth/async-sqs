package com.bandwidth.sqs.client;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.StaxResponseHandler;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.transform.SendMessageRequestMarshaller;
import com.amazonaws.util.StringInputStream;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class AsyncRequestSenderTest {
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/account_id/queue_name";
    private static final String MESSAGE_BODY = "this is a message body";
    private static final String ERROR_RESPONSE_SENDER_ERROR = "<ErrorResponse>\n"
            + "   <Error>\n"
            + "      <Type>\n"
            + "         Sender\n"
            + "      </Type>\n"
            + "      <Code>\n"
            + "         MissingAction\n"
            + "      </Code>\n"
            + "      <Message>\n"
            + "         Value (quename_nonalpha) for parameter QueueName is invalid.\n"
            + "         Must be an alphanumeric String of 1 to 80 in length\n"
            + "      </Message>\n"
            + "   </Error>\n"
            + "   <RequestId>\n"
            + "      42d59b56-7407-4c4a-be0f-4c88daeea257\n"
            + "   </RequestId>\n"
            + "</ErrorResponse>";

    private final AWSCredentialsProvider awsCredentialsProviderMock = mock(
            AWSCredentialsProvider.class);
    private final AsyncRequestConverter requestAdapterMock = mock(AsyncRequestConverter.class);
    private final AsyncResponseConverter responseAdapterMock = mock(AsyncResponseConverter.class);
    private final AsyncHttpClient httpClientMock = mock(AsyncHttpClient.class);
    private final SendMessageRequestMarshaller requestMarshallerMock = mock(
            SendMessageRequestMarshaller.class);

    private final StaxResponseHandler<SendMessageResult> staxResponseHandlerMock = mock(StaxResponseHandler.class);

    private final Request awsHttpRequestMock = mock(Request.class);
    private final AmazonWebServiceResponse awsResponseMock = mock(AmazonWebServiceResponse.class);
    private final Response responseMock = mock(Response.class);
    private final HttpResponse httpResponseMock = mock(HttpResponse.class);
    private final AWS4Signer requestSignerMock = mock(AWS4Signer.class);
    private final SendMessageResult sendMessageResultMock = mock(SendMessageResult.class);

    private final AsyncRequestSender<SendMessageRequest, SendMessageResult> sender =
            new AsyncRequestSender<>(awsCredentialsProviderMock, requestAdapterMock, responseAdapterMock,
                    httpClientMock,
                    requestSignerMock, requestMarshallerMock, staxResponseHandlerMock, SendMessageRequest::getQueueUrl);

    private final SendMessageRequest request = new SendMessageRequest()
            .withQueueUrl(QUEUE_URL)
            .withMessageBody(MESSAGE_BODY);

    @Before
    public void init() throws Exception {
        when(requestMarshallerMock.marshall(any())).thenReturn(awsHttpRequestMock);
        when(responseAdapterMock.apply(any(), any())).thenReturn(httpResponseMock);
        when(httpResponseMock.getStatusCode()).thenReturn(200);
        when(httpResponseMock.getRequest()).thenReturn(awsHttpRequestMock);
        when(httpResponseMock.getContent()).thenReturn(
                new StringInputStream(ERROR_RESPONSE_SENDER_ERROR));
        when(staxResponseHandlerMock.handle(any())).thenReturn(awsResponseMock);
        when(awsResponseMock.getResult()).thenReturn(sendMessageResultMock);

        doAnswer(invocation -> {
            AsyncCompletionHandler handler = invocation.getArgumentAt(1,
                    AsyncCompletionHandler.class);
            handler.onCompleted(responseMock);
            return null;
        }).when(httpClientMock).executeRequest((org.asynchttpclient.Request) any(), any());
    }

    @Test
    public void asyncRequest200ResponseTest() {
        sender.send(request).test().assertComplete();
        verify(httpClientMock).executeRequest(any(org.asynchttpclient.Request.class), any());
    }

    @Test
    public void asyncRequest500ResponseTest() {
        when(httpResponseMock.getStatusCode()).thenReturn(500);

        sender.send(request).test().assertError(Throwable.class);
        verify(httpClientMock).executeRequest(any(org.asynchttpclient.Request.class), any());
    }

    @Test
    public void httpClientOnThrowableTest() {
        Exception exception = new RuntimeException();
        doAnswer(invocation -> {
            AsyncCompletionHandler handler = invocation.getArgumentAt(1,
                    AsyncCompletionHandler.class);
            handler.onThrowable(exception);
            return null;
        }).when(httpClientMock).executeRequest((org.asynchttpclient.Request) any(), any());
        sender.send(request).test().assertError(exception);
    }
}
