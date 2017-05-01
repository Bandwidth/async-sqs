package com.bandwidth.sqs.action.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.SignableRequest;
import com.amazonaws.http.HttpMethodName;

import org.asynchttpclient.Request;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.netty.handler.codec.http.HttpHeaders;


public class AsyncRequestConverterTest {
    private static final String TEST_KEY_A = "a";
    private static final String TEST_VALUE_B = "b";
    private static final String TEST_KEY_C = "c";
    private static final String TEST_VALUE_D = "d";
    private static final String ENCODED_BODY = "a=b&c=d";
    private static final String GET_METHOD = "GET";
    private static final String PATH = "/path";

    @Test
    public void testRequestConversion() throws URISyntaxException {


        Map<String, String> headers = new HashMap<>();
        headers.put(TEST_KEY_A, TEST_VALUE_B);
        headers.put(TEST_KEY_C, TEST_VALUE_D);

        Map<String, List<String>> params = new HashMap<>();
        params.put(TEST_KEY_A, Collections.singletonList(TEST_VALUE_B));
        params.put(TEST_KEY_C, Collections.singletonList(TEST_VALUE_D));

        SignableRequest<?> signableRequest = mock(SignableRequest.class);
        URI endpoint = new URI("http://bandwidth.com");
        when(signableRequest.getHttpMethod()).thenReturn(HttpMethodName.GET);
        when(signableRequest.getEndpoint()).thenReturn(endpoint);
        when(signableRequest.getResourcePath()).thenReturn(PATH);
        when(signableRequest.getHeaders()).thenReturn(headers);
        when(signableRequest.getParameters()).thenReturn(params);

        Function<SignableRequest<?>, Request> converter = new AsyncRequestConverter();
        Request output = converter.apply(signableRequest);

        assertThat(output.getMethod()).isEqualToIgnoringCase(GET_METHOD);
        assertThat(output.getUri().getPath()).isEqualTo(PATH);
        assertThat(output.getHeaders().get(TEST_KEY_A)).isEqualTo(TEST_VALUE_B);
        assertThat(output.getHeaders().get(TEST_KEY_C)).isEqualTo(TEST_VALUE_D);
        assertThat(output.getStringData()).isEqualTo(ENCODED_BODY);
        assertThat(output.getHeaders().get(HttpHeaders.Names.CONTENT_TYPE))
                .isEqualTo(HttpHeaders.Values.APPLICATION_X_WWW_FORM_URLENCODED);
    }
}
