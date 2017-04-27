package com.bandwidth.sqs.actions;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.bandwidth.sqs.queue.SqsQueueAttributes;

import org.junit.Test;

public class SetQueueAttributesActionTest {
    private static final String QUEUE_URL = "https://domain.com/path";
    private static final SqsQueueAttributes ATTRIBUTES = SqsQueueAttributes.builder().build();


    @Test
    public void testCreateRequest() {
        SetQueueAttributesRequest request = SetQueueAttributesAction.createRequest(QUEUE_URL, ATTRIBUTES);
        assertThat(request.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(request.getAttributes().size()).isGreaterThan(0);
    }

    @Test
    public void testConstructor() {
        assertThat(new SetQueueAttributesAction(QUEUE_URL, ATTRIBUTES)).isNotNull();
    }

}
