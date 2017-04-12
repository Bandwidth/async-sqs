package com.bandwidth.sqs.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.http.HttpResponse;

import org.asynchttpclient.Response;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

public class AsyncResponseAdapterTest {
    private static final String TEST_KEY_A = "a";
    private static final String TEST_VALUE_B = "b";
    private static final String TEST_KEY_C = "c";
    private static final String TEST_VALUE_D = "d";
    private static final int STATUS_CODE = 200;
    private static final String STATUS_TEXT = "Ok";
    private static final String ENCODED_BODY = "a=b&c=d";

    @Test
    public void testConvert() throws IOException {
        Response responseMock = mock(Response.class);

        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(TEST_KEY_A, TEST_VALUE_B);
        headers.add(TEST_KEY_C, TEST_VALUE_D);

        when(responseMock.getHeaders()).thenReturn(headers);
        when(responseMock.getStatusCode()).thenReturn(STATUS_CODE);
        when(responseMock.getStatusText()).thenReturn(STATUS_TEXT);
        when(responseMock.getResponseBodyAsStream()).thenReturn(
                new ByteArrayInputStream(ENCODED_BODY.getBytes(StandardCharsets.UTF_8))
        );

        HttpResponse awsResponse = new AsyncResponseConverter().apply(responseMock, null);

        assertThat(awsResponse.getHeaders().get(TEST_KEY_A)).isEqualTo(TEST_VALUE_B);
        assertThat(awsResponse.getHeaders().get(TEST_KEY_C)).isEqualTo(TEST_VALUE_D);
        assertThat(awsResponse.getStatusCode()).isEqualTo(STATUS_CODE);
        assertThat(awsResponse.getStatusText()).isEqualTo(STATUS_TEXT);
        assertThat(new BufferedReader(new InputStreamReader(awsResponse.getContent())).readLine())
                .isEqualTo(ENCODED_BODY);
    }

    @Test
    public void testInstantiate() {
        new AsyncResponseConverter();//workaround for a bug in Cobertura
    }
}
