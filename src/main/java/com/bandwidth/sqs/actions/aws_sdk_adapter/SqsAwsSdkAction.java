package com.bandwidth.sqs.actions.aws_sdk_adapter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.DefaultErrorResponseHandler;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.StaxResponseHandler;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.transform.StandardErrorUnmarshaller;
import com.amazonaws.transform.StaxUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import com.bandwidth.sqs.actions.SqsAction;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.net.URI;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An action that uses Amazon's SDK to process a request and response
 */
public class SqsAwsSdkAction<RequestT extends AmazonWebServiceRequest, ResponseT> implements SqsAction<ResponseT> {

    static final String SHORT_SERVICE_NAME = "sqs";
    private static final String SCHEME_SEPERATOR = "://";
    private static final Range<Integer> HTTP_STATUS_RANGE_SUCCESS = Range.closed(200, 299);


    private final String requestUrl;
    private final RequestT request;
    private final Marshaller<com.amazonaws.Request<RequestT>, RequestT> marshaller;

    private StaxResponseHandler<ResponseT> staxResponseHandler;
    private com.amazonaws.Request<RequestT> awsHttpRequest;
    private AWS4Signer requestSigner = new AWS4Signer();
    private Function<SignableRequest<?>, Request> requestConverter = new AsyncRequestConverter();
    private BiFunction<Response, com.amazonaws.Request, HttpResponse> responseConverter = new AsyncResponseConverter();
    private DefaultErrorResponseHandler errorResponseHandler =
            new DefaultErrorResponseHandler(ImmutableList.of(new StandardErrorUnmarshaller(AmazonSQSException.class)));

    public SqsAwsSdkAction(RequestT request, String requestUrl,
            Marshaller<com.amazonaws.Request<RequestT>, RequestT> marshaller,
            Unmarshaller<ResponseT, StaxUnmarshallerContext> unmarshaller) {

        this.requestUrl = requestUrl;
        this.request = request;
        this.marshaller = marshaller;
        this.staxResponseHandler = new StaxResponseHandler<>(unmarshaller);
    }

    @Override
    public ResponseT parseHttpResponse(Response response) throws Exception {
        HttpResponse httpResponse = responseConverter.apply(response, awsHttpRequest);
        int statusCode = httpResponse.getStatusCode();
        if (HTTP_STATUS_RANGE_SUCCESS.contains(statusCode)) {
            AmazonWebServiceResponse<ResponseT> awsResponse = staxResponseHandler.handle(httpResponse);
            return awsResponse.getResult();
        } else {
            throw errorResponseHandler.handle(httpResponse);
        }
    }

    @Override
    public Request toHttpRequest(AWSCredentials credentials) {
        URI fullUri = URI.create(requestUrl);
        URI endpoint = URI.create(fullUri.getScheme() + SCHEME_SEPERATOR + fullUri.getHost());

        awsHttpRequest = marshaller.marshall(request);
        awsHttpRequest.setEndpoint(endpoint);
        awsHttpRequest.setResourcePath(fullUri.getPath());
        requestSigner.setServiceName(SHORT_SERVICE_NAME);

        requestSigner.sign(awsHttpRequest, credentials);
        return requestConverter.apply(awsHttpRequest);
    }

    @VisibleForTesting
    void setRequestConverter(AsyncRequestConverter converter) {
        this.requestConverter = converter;
    }

    @VisibleForTesting
    void setResponseConverter(AsyncResponseConverter converter) {
        this.responseConverter = converter;
    }

    @VisibleForTesting
    void setRequestSigner(AWS4Signer signer) {
        this.requestSigner = signer;
    }

    @VisibleForTesting
    void setStaxResponseHandler(StaxResponseHandler<ResponseT> handler){
        this.staxResponseHandler = handler;
    }

    @VisibleForTesting
    void setErrorResponseHandler(DefaultErrorResponseHandler handler){
        this.errorResponseHandler = handler;
    }
}
