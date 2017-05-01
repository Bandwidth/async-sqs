package com.bandwidth.sqs.actions;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.bandwidth.sqs.queue.entry.ChangeMessageVisibilityEntry;

import org.junit.Test;

import java.time.Duration;
import java.util.Map;

public class ChangeMessageVisibilityBatchActionTest {

    private static final String QUEUE_URL = "http://domain.com/path";

    private static final ChangeMessageVisibilityEntry ENTRY = ChangeMessageVisibilityEntry.builder()
            .receiptHandle("receipt handle")
            .newVisibilityTimeout(Duration.ZERO)
            .build();

    private static final Map<String, ChangeMessageVisibilityEntry> ENTRY_MAP =
            new ImmutableMap.Builder<String,
                    ChangeMessageVisibilityEntry>()
                    .put("0", ENTRY)
                    .put("1", ENTRY)
                    .build();

    @Test
    public void testCreateRequest() {
        ChangeMessageVisibilityBatchRequest request =
                ChangeMessageVisibilityBatchAction.createRequest(QUEUE_URL, ENTRY_MAP);
        assertThat(request.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(request.getEntries().size()).isEqualTo(ENTRY_MAP.size());
    }

    @Test
    public void testConstructor() {
        assertThat(new ChangeMessageVisibilityBatchAction(QUEUE_URL, ENTRY_MAP)).isNotNull();
    }
}
