package com.bandwidth.sqs.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.amazonaws.auth.AWSCredentialsProvider;

import org.asynchttpclient.AsyncHttpClient;
import org.junit.Test;

import io.reactivex.functions.Function;

public class SqsClientBuilderTest {
    private static final int RETRY_COUNT = 99;

    private final AWSCredentialsProvider credentialsProviderMock = mock(AWSCredentialsProvider.class);
    private final AsyncHttpClient asyncHttpClientMock = mock(AsyncHttpClient.class);

    @Test
    public void testBuilder() {
        SqsClientBuilder stringClientBuilder = SqsClient.builder()
                .credentialsProvider(credentialsProviderMock)
                .httpClient(asyncHttpClientMock)
                .retryCount(RETRY_COUNT);
        SqsClient client = stringClientBuilder.build();
        assertThat(client).isNotNull();
    }
}
