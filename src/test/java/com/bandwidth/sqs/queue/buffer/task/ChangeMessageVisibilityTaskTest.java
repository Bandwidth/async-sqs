package com.bandwidth.sqs.queue.buffer.task;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResultEntry;
import com.bandwidth.sqs.actions.sender.SqsRequestSender;
import com.bandwidth.sqs.queue.entry.ChangeMessageVisibilityEntry;

import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import io.reactivex.Single;

public class ChangeMessageVisibilityTaskTest {
    private static final String QUEUE_URL = "http://domain.com/path";
    private static final String MESSAGE_BODY = "message-body";
    private static final String MESSAGE_ID = "message-id";
    private static final String RECEIPT_HANDLE = "receipt-handle";
    private static final Exception EXCEPTION = new RuntimeException("error");
    private static final ChangeMessageVisibilityBatchResult SUCCESS_RESULT =
            new ChangeMessageVisibilityBatchResult().withSuccessful(
                    new ChangeMessageVisibilityBatchResultEntry().withId("0"),
                    new ChangeMessageVisibilityBatchResultEntry().withId("1")
            );

    private static final ChangeMessageVisibilityBatchResult ERROR_RESULT =
            new ChangeMessageVisibilityBatchResult().withFailed(
                    new BatchResultErrorEntry().withId("0"),
                    new BatchResultErrorEntry().withId("1")
            );

    private final ChangeMessageVisibilityEntry ENTRY_1 = ChangeMessageVisibilityEntry.builder()
            .receiptHandle(RECEIPT_HANDLE).newVisibilityTimeout(Duration.ZERO).build();
    private final ChangeMessageVisibilityEntry ENTRY_2 = ChangeMessageVisibilityEntry.builder()
            .receiptHandle(RECEIPT_HANDLE).newVisibilityTimeout(Duration.ZERO).build();
    private final Map<String, ChangeMessageVisibilityEntry> ENTRY_MAP = ImmutableMap.of(
            "0", ENTRY_1,
            "1", ENTRY_2
    );
    private final SqsRequestSender requestSenderMock = mock(SqsRequestSender.class);

    private final Task<String, ChangeMessageVisibilityEntry> task = new ChangeMessageVisibilityTask(requestSenderMock);

    public ChangeMessageVisibilityTaskTest() {
        when(requestSenderMock.sendRequest(any())).thenReturn(Single.just(SUCCESS_RESULT));
    }

    @Test
    public void testTaskSuccess() {
        task.run(QUEUE_URL, ENTRY_MAP);
        ENTRY_MAP.values().forEach((entry) -> entry.getResultSubject().test().assertComplete());
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
