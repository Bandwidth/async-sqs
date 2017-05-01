package com.bandwidth.sqs.queue.buffer.task;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResultEntry;
import com.bandwidth.sqs.action.sender.SqsRequestSender;
import com.bandwidth.sqs.queue.entry.DeleteMessageEntry;

import org.junit.Test;

import java.util.Map;

import io.reactivex.Single;

public class DeleteMessageTaskTest {
    private static final String QUEUE_URL = "http://domain.com/path";
    private static final String RECEIPT_HANDLE = "receipt-handle";
    private static final Exception EXCEPTION = new RuntimeException("error");
    private static final DeleteMessageBatchResult SUCCESS_RESULT = new DeleteMessageBatchResult().withSuccessful(
            new DeleteMessageBatchResultEntry().withId("0"),
            new DeleteMessageBatchResultEntry().withId("1")
    );

    private static final DeleteMessageBatchResult ERROR_RESULT = new DeleteMessageBatchResult().withFailed(
            new BatchResultErrorEntry().withId("0"),
            new BatchResultErrorEntry().withId("1")
    );

    private final DeleteMessageEntry ENTRY_1 = DeleteMessageEntry.builder()
            .receiptHandle(RECEIPT_HANDLE).build();
    private final DeleteMessageEntry ENTRY_2 = DeleteMessageEntry.builder()
            .receiptHandle(RECEIPT_HANDLE).build();
    private final Map<String, DeleteMessageEntry> ENTRY_MAP = ImmutableMap.of(
            "0", ENTRY_1,
            "1", ENTRY_2
    );

    private final SqsRequestSender requestSenderMock = mock(SqsRequestSender.class);

    private final Task<String, DeleteMessageEntry> task = new DeleteMessageTask(requestSenderMock);

    public DeleteMessageTaskTest() {
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
