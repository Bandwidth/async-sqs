package com.bandwidth.sqs.action;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.bandwidth.sqs.queue.entry.DeleteMessageEntry;

import org.junit.Test;

import java.util.Map;

public class DeleteMessageBatchActionTest {

    private static final String QUEUE_URL = "https://domain.com/path";

    private static final DeleteMessageEntry ENTRY = DeleteMessageEntry.builder()
            .receiptHandle("receipt handle")
            .build();

    private static final Map<String, DeleteMessageEntry> ENTRY_MAP = new ImmutableMap.Builder<String,
            DeleteMessageEntry>()
            .put("0", ENTRY)
            .put("1", ENTRY)
            .build();

    @Test
    public void testCreateRequest() {
        DeleteMessageBatchRequest request = DeleteMessageBatchAction.createRequest(QUEUE_URL, ENTRY_MAP);
        assertThat(request.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(request.getEntries().size()).isEqualTo(ENTRY_MAP.size());
    }

    @Test
    public void testConstructor() {
        assertThat(new DeleteMessageBatchAction(QUEUE_URL, ENTRY_MAP)).isNotNull();
    }
}
