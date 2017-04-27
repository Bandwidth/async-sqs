package com.bandwidth.sqs.actions.aws_sdk_adapter;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.DefaultErrorResponseHandler;
import com.amazonaws.http.StaxResponseHandler;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.transform.StaxUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Test;

import java.net.URI;

@SuppressWarnings("unchecked")
public class SqsAwsSdkActionTest {

    private static final String REQUEST_URL = "http://domain.com/path";
    private static final String ACCESS_KEY_ID = "access-key-id";
    private static final String SECRET_KEY = "secret-key";
    private static final int STATUS_SUCCESS = 200;
    private static final int STATUS_FAILED = 500;


    private final SendMessageRequest sendMessageRequestMock = mock(SendMessageRequest.class);
    private final SendMessageResult sendMessageResultMock = mock(SendMessageResult.class);
    private final Marshaller<com.amazonaws.Request<SendMessageRequest>, SendMessageRequest> marshallerMock =
            mock(Marshaller.class);
    private final Unmarshaller<SendMessageResult, StaxUnmarshallerContext> unmarshallerMock = mock(Unmarshaller.class);
    private final AWSCredentials credentialsMock = mock(AWSCredentials.class);
    private final com.amazonaws.Request<SendMessageRequest> requestMock = mock(com.amazonaws.Request.class);
    private final Response responseMock = mock(Response.class);
    private final com.amazonaws.http.HttpResponse awsResponseMock = mock(com.amazonaws.http.HttpResponse.class);
    private final AmazonWebServiceResponse<SendMessageResult> awsWebResponseMock = mock(AmazonWebServiceResponse.class);
    private final AsyncRequestConverter requestConverterMock = mock(AsyncRequestConverter.class);
    private final AsyncResponseConverter responseConverterMock = mock(AsyncResponseConverter.class);
    private final AWS4Signer signerMock = mock(AWS4Signer.class);
    private final StaxResponseHandler<SendMessageResult> staxResponseHandlerMock = mock(StaxResponseHandler.class);
    private final DefaultErrorResponseHandler errorHandlerMock = mock(DefaultErrorResponseHandler.class);
    private final AmazonServiceException awsExceptionMock = mock(AmazonServiceException.class);


    private final SqsAwsSdkAction<SendMessageRequest, SendMessageResult> action =
            new SqsAwsSdkAction<>(sendMessageRequestMock, REQUEST_URL, marshallerMock, unmarshallerMock);

    public SqsAwsSdkActionTest() throws Exception {
        action.setRequestConverter(requestConverterMock);
        action.setResponseConverter(responseConverterMock);
        action.setRequestSigner(signerMock);
        action.setStaxResponseHandler(staxResponseHandlerMock);
        action.setErrorResponseHandler(errorHandlerMock);

        when(marshallerMock.marshall(any())).thenReturn(requestMock);
        when(credentialsMock.getAWSAccessKeyId()).thenReturn(ACCESS_KEY_ID);
        when(credentialsMock.getAWSSecretKey()).thenReturn(SECRET_KEY);
        when(responseConverterMock.apply(any(), any())).thenReturn(awsResponseMock);
        when(staxResponseHandlerMock.handle(any())).thenReturn(awsWebResponseMock);
        when(awsResponseMock.getStatusCode()).thenReturn(STATUS_SUCCESS);
        when(awsWebResponseMock.getResult()).thenReturn(sendMessageResultMock);
        when(errorHandlerMock.handle(any())).thenReturn(awsExceptionMock);
    }

    @Test
    public void testToHttpRequest() {
        action.toHttpRequest(credentialsMock);

        verify(requestMock).setEndpoint(URI.create("http://domain.com"));
        verify(requestMock).setResourcePath("/path");
        verify(signerMock).setServiceName(SqsAwsSdkAction.SHORT_SERVICE_NAME);
        verify(signerMock).sign(requestMock, credentialsMock);
        verify(requestConverterMock).apply(requestMock);
    }

    @Test
    public void testParseHttpResponseSuccess() throws Exception {
        assertThat(action.parseHttpResponse(responseMock)).isEqualTo(sendMessageResultMock);
    }

    @Test
    public void testParseHttpResponseFailure() throws Exception {
        when(awsResponseMock.getStatusCode()).thenReturn(STATUS_FAILED);
        assertThatThrownBy(() -> action.parseHttpResponse(responseMock)).isInstanceOf(AmazonServiceException.class);
    }
}
