package com.bandwidth.sqs.action;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.bandwidth.sqs.queue.MutableSqsQueueAttributes;

import org.junit.Test;

public class SetQueueAttributesActionTest {
    private static final String QUEUE_URL = "https://domain.com/path";
    private static final MutableSqsQueueAttributes ATTRIBUTES = MutableSqsQueueAttributes.builder().build();


    @Test
    public void testCreateRequest() {
        SetQueueAttributesRequest request = SetQueueAttributesAction.createRequest(QUEUE_URL, ATTRIBUTES);
        assertThat(request.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(request.getAttributes()).isEmpty();
    }

    @Test
    public void testConstructor() {
        assertThat(new SetQueueAttributesAction(QUEUE_URL, ATTRIBUTES)).isNotNull();
    }

}
