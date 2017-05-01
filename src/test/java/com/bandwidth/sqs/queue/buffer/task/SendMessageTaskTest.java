package com.bandwidth.sqs.queue.buffer.task;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.bandwidth.sqs.action.sender.SqsRequestSender;
import com.bandwidth.sqs.queue.entry.SendMessageEntry;

import org.junit.Test;

import java.util.Map;

import io.reactivex.Single;

public class SendMessageTaskTest {
    private static final String QUEUE_URL = "http://domain.com/path";
    private static final String MESSAGE_BODY = "message-body";
    private static final String MESSAGE_ID = "message-id";
    private static final Exception EXCEPTION = new RuntimeException("error");
    private static final SendMessageBatchResult SUCCESS_RESULT = new SendMessageBatchResult().withSuccessful(
            new SendMessageBatchResultEntry().withId("0").withMessageId(MESSAGE_ID),
            new SendMessageBatchResultEntry().withId("1").withMessageId(MESSAGE_ID)
    );

    private static final SendMessageBatchResult ERROR_RESULT = new SendMessageBatchResult().withFailed(
            new BatchResultErrorEntry().withId("0"),
            new BatchResultErrorEntry().withId("1")
    );

    private final SendMessageEntry ENTRY_1 = SendMessageEntry.builder()
            .body(MESSAGE_BODY).build();
    private final SendMessageEntry ENTRY_2 = SendMessageEntry.builder()
            .body(MESSAGE_BODY).build();
    private final Map<String, SendMessageEntry> ENTRY_MAP = ImmutableMap.of(
            "0", ENTRY_1,
            "1", ENTRY_2
    );
    private final SqsRequestSender requestSenderMock = mock(SqsRequestSender.class);

    private final Task<String, SendMessageEntry> task = new SendMessageTask(requestSenderMock);

    public SendMessageTaskTest() {
        when(requestSenderMock.sendRequest(any())).thenReturn(Single.just(SUCCESS_RESULT));
    }

    @Test
    public void testTaskSuccess() {
        task.run(QUEUE_URL, ENTRY_MAP);
        ENTRY_MAP.values().forEach((entry) -> entry.getResultSubject().test().assertValue(MESSAGE_ID));
        verify(requestSenderMock).sendRequest(any());
    }

    @Test
    public void testTaskIndividualError() {
        when(requestSenderMock.sendRequest(any())).thenReturn(Single.just(ERROR_RESULT));
        task.run(QUEUE_URL, ENTRY_MAP);
        ENTRY_MAP.values().forEach((entry) -> entry.getResultSubject().test().assertError(Exception.class));
        verify(requestSenderMock).sendRequest(any());
    }

    @Test
    public void testTaskError() {
        when(requestSenderMock.sendRequest(any())).thenReturn(Single.error(EXCEPTION));
        task.run(QUEUE_URL, ENTRY_MAP);
        ENTRY_MAP.values().forEach((entry) -> entry.getResultSubject().test().assertError(EXCEPTION));
        verify(requestSenderMock).sendRequest(any());
    }
}
