package com.bandwidth.sqs.actions;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;

import org.junit.Test;

import java.util.Collections;

public class GetQueueAttributesActionTest {
    private static final String QUEUE_URL = "http://domain.com/path";

    @Test
    public void testCreateRequest() {
        GetQueueAttributesRequest request = GetQueueAttributesAction.createRequest(QUEUE_URL);
        assertThat(request.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(request.getAttributeNames()).isEqualTo(Collections.singletonList("All"));
    }

    @Test
    public void testConstructor() {
        assertThat(new GetQueueAttributesAction(QUEUE_URL)).isNotNull();
    }
}
