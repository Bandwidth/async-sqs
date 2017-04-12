package com.bandwidth.sqs.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.Request;
import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.DefaultErrorResponseHandler;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.StaxResponseHandler;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.transform.StandardErrorUnmarshaller;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;

import java.net.URI;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.reactivex.Single;
import io.reactivex.subjects.SingleSubject;

/**
 * Converts a request to an async request, sends it, then converts it back
 *
 * @param <Q> Re(q)uest object
 * @param <S> Re(s)ponse object
 */
public class AsyncRequestSender<Q extends AmazonWebServiceRequest, S> {
    private static final String SHORT_SERVICE_NAME = "sqs";
    private static final String SCHEME_SEPERATOR = "://";

    private static final Range<Integer> HTTP_STATUS_RANGE_SUCCESS = Range.closed(200, 299);

    private static final DefaultErrorResponseHandler ERROR_RESPONSE_HANDLER =
            new DefaultErrorResponseHandler(
                    ImmutableList.of(new StandardErrorUnmarshaller(AmazonSQSException.class)));

    private final AWSCredentialsProvider credentialsProvider;
    private final Function<SignableRequest<?>, org.asynchttpclient.Request> requestAdapter;
    private final BiFunction<Response, Request, HttpResponse> responseAdapter;
    private final AsyncHttpClient httpClient;
    private final AWS4Signer requestSigner;
    private final Marshaller<Request<Q>, Q> marshaller;
    private final StaxResponseHandler<S> staxResponseHandler;

    private final Function<Q, String> getQueueUrl;

    public AsyncRequestSender(AWSCredentialsProvider credentialsProvider,
            Function<SignableRequest<?>, org.asynchttpclient.Request> requestAdapter,
            BiFunction<Response, Request, HttpResponse> responseAdapter, AsyncHttpClient httpClient,
            AWS4Signer requestSigner, Marshaller<Request<Q>, Q> marshaller,
            StaxResponseHandler<S> staxResponseHandler,
            Function<Q, String> getQueueUrl) {
        this.credentialsProvider = credentialsProvider;
        this.requestAdapter = requestAdapter;
        this.responseAdapter = responseAdapter;
        this.httpClient = httpClient;
        this.requestSigner = requestSigner;
        this.marshaller = marshaller;
        this.getQueueUrl = getQueueUrl;
        this.staxResponseHandler = staxResponseHandler;
    }

    public Single<S> send(Q request) {
        com.amazonaws.Request<Q> awsHttpRequest = marshaller.marshall(request);
        org.asynchttpclient.Request asyncRequest = toAsyncRequest(awsHttpRequest, getQueueUrl.apply(request));

        SingleSubject<S> singleSubject = SingleSubject.create();
        httpClient.executeRequest(asyncRequest, new AsyncCompletionHandler<Response>() {
            @Override
            public Response onCompleted(Response response) throws Exception {

                //Convert Async Http Request back to AWS request
                HttpResponse httpResponse = responseAdapter.apply(response, awsHttpRequest);
                int statusCode = httpResponse.getStatusCode();
                if (HTTP_STATUS_RANGE_SUCCESS.contains(statusCode)) {
                    AmazonWebServiceResponse<S> awsResponse = staxResponseHandler.handle(httpResponse);
                    singleSubject.onSuccess(awsResponse.getResult());
                } else {
                    singleSubject.onError(ERROR_RESPONSE_HANDLER.handle(httpResponse));
                }
                return response;
            }

            @Override
            public void onThrowable(Throwable throwable) {
                singleSubject.onError(throwable);
            }
        });
        return singleSubject;
    }

    private org.asynchttpclient.Request toAsyncRequest(com.amazonaws.Request<Q> awsHttpRequest, String queueUrl) {
        URI fullUri = URI.create(queueUrl);
        URI endpoint = URI.create(fullUri.getScheme() + SCHEME_SEPERATOR + fullUri.getHost());

        awsHttpRequest.setEndpoint(endpoint);
        awsHttpRequest.setResourcePath(fullUri.getPath());
        requestSigner.setServiceName(SHORT_SERVICE_NAME);

        requestSigner.sign(awsHttpRequest, credentialsProvider.getCredentials());
        return requestAdapter.apply(awsHttpRequest);
    }
}