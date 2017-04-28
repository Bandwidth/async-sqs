package com.bandwidth.sqs.actions;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.bandwidth.sqs.queue.entry.SendMessageEntry;

import org.junit.Test;

import java.util.Map;
import java.util.Optional;

public class SendMessageBatchActionTest {
    private static final String QUEUE_URL = "https://domain.com/path";

    private static final SendMessageEntry ENTRY = SendMessageEntry.builder()
            .body("message body")
            .delay(Optional.empty())
            .build();

    private static final Map<String, SendMessageEntry> ENTRY_MAP = new ImmutableMap.Builder<String, SendMessageEntry>()
            .put("0", ENTRY)
            .put("1", ENTRY)
            .build();


    @Test
    public void testCreateRequest() {
        SendMessageBatchRequest request = SendMessageBatchAction.createRequest(QUEUE_URL, ENTRY_MAP);
        assertThat(request.getQueueUrl()).isEqualTo(QUEUE_URL);
        assertThat(request.getEntries().size()).isEqualTo(ENTRY_MAP.size());
    }

    @Test
    public void testConstructor() {
        assertThat(new SendMessageBatchAction(QUEUE_URL, ENTRY_MAP)).isNotNull();
    }
}
