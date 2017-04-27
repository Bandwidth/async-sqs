package com.bandwidth.sqs.actions.aws_sdk_adapter;

import com.amazonaws.Request;
import com.amazonaws.http.HttpResponse;

import org.asynchttpclient.Response;

import java.util.function.BiFunction;

/**
 * Converts an Async Http Response to an AWS Response object
 */
public class AsyncResponseConverter implements BiFunction<Response, Request, HttpResponse> {
    public HttpResponse apply(Response asyncResponse, Request originalRequest) {
        HttpResponse output = new HttpResponse(originalRequest, null);
        output.setStatusCode(asyncResponse.getStatusCode());
        output.setStatusText(asyncResponse.getStatusText());
        output.setContent(asyncResponse.getResponseBodyAsStream());
        asyncResponse.getHeaders().forEach((header) -> output.addHeader(header.getKey(), header.getValue()));
        return output;
    }
}