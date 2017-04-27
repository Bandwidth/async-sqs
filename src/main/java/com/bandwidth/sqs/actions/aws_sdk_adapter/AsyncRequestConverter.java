package com.bandwidth.sqs.actions.aws_sdk_adapter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.amazonaws.SignableRequest;
import com.amazonaws.util.SdkHttpUtils;

import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import java.util.function.Function;

import io.netty.handler.codec.http.HttpHeaders;

/**
 * Converts an AWS SignableRequest object to an Async Http Request
 */
public class AsyncRequestConverter implements Function<SignableRequest<?>, Request> {
    public Request apply(SignableRequest<?> signableRequest) {
        Multimap<String, String> headers = ArrayListMultimap.create(Multimaps.forMap(signableRequest.getHeaders()));
        headers.put(HttpHeaders.Names.CONTENT_TYPE, HttpHeaders.Values.APPLICATION_X_WWW_FORM_URLENCODED);
        return new RequestBuilder()
                .setMethod(signableRequest.getHttpMethod().name())
                .setUrl(signableRequest.getEndpoint().resolve(signableRequest.getResourcePath()).toString())
                .setHeaders(headers.asMap())
                .setBody(SdkHttpUtils.encodeParameters(signableRequest))
                .build();
    }
}