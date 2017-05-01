package com.bandwidth.sqs.client;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.bandwidth.sqs.action.sender.BaseSqsRequestSender;
import com.bandwidth.sqs.action.sender.RetryingSqsRequestSender;
import com.bandwidth.sqs.action.sender.SqsRequestSender;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

public class SqsClientBuilder {
    public static final int DEFAULT_RETRY_COUNT = 3;
    public static final AWSCredentialsProvider DEFAULT_CREDENTIALS_PROVIDER = new DefaultAWSCredentialsProviderChain();
    public static final AsyncHttpClient DEFAULT_ASYNC_HTTP_CLIENT = new DefaultAsyncHttpClient(
            new DefaultAsyncHttpClientConfig.Builder()
                    .setKeepAlive(true)
                    .setConnectionTtl(10000)//10 seconds
                    .setMaxConnections(Integer.MAX_VALUE)
                    .setMaxConnectionsPerHost(Integer.MAX_VALUE)
                    .build()
    );

    private int retryCount = DEFAULT_RETRY_COUNT;
    private AWSCredentialsProvider credentialsProvider = DEFAULT_CREDENTIALS_PROVIDER;
    private AsyncHttpClient httpClient = DEFAULT_ASYNC_HTTP_CLIENT;

    public SqsClientBuilder retryCount(int retryCount) {
        this.retryCount = retryCount;
        return this;
    }

    public SqsClientBuilder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        return this;
    }

    public SqsClientBuilder httpClient(AsyncHttpClient asyncHttpClient) {
        this.httpClient = asyncHttpClient;
        return this;
    }

    public SqsClient build() {
        SqsRequestSender requestSender = new RetryingSqsRequestSender(retryCount,
                new BaseSqsRequestSender(httpClient, credentialsProvider));
        return new SqsClient(requestSender);
    }
}
