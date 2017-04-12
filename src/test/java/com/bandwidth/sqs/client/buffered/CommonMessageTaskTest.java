package com.bandwidth.sqs.client.buffered;


import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.bandwidth.sqs.client.BatchRequestEntry;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import io.reactivex.Single;
import io.reactivex.subjects.SingleSubject;

@SuppressWarnings("unchecked")
public class CommonMessageTaskTest {
    private static final String QUEUE_URL = "";
    private static final String ID_0 = "0";
    private static final String MESSAGE_BODY = "This is a message body";

    private static final SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry()
            .withMessageBody(MESSAGE_BODY);

    private static final BatchResult<SendMessageBatchResultEntry> BATCH_RESULT_SUCCESS = new BatchResult<>(
            Collections.singletonList(new SendMessageBatchResultEntry().withId(ID_0)),
            Collections.emptyList()
    );

    private static final BatchResult<SendMessageBatchResultEntry> BATCH_RESULT_CLIENT_ERROR = new BatchResult<>(
            Collections.emptyList(),
            Collections.singletonList(new BatchResultErrorEntry().withId(ID_0).withSenderFault(true))
    );

    private static final BatchResult<SendMessageBatchResultEntry> BATCH_RESULT_SERVER_ERROR = new BatchResult<>(
            Collections.emptyList(),
            Collections.singletonList(new BatchResultErrorEntry().withId(ID_0).withSenderFault(false))
    );

    private final Function<BatchRequest<SendMessageBatchRequestEntry>,
            Single<BatchResult<SendMessageBatchResultEntry>>> sendBatchRequestMock = mock(Function.class);
    private final BatchResult<SendMessageBatchResultEntry> batchResultSuccess = mock(BatchResult.class);
    private final List<BatchRequestEntry<SendMessageBatchRequestEntry, SendMessageResult>> list = new ArrayList<>();
    private final SingleSubject<SendMessageResult> singleSubject = SingleSubject.create();


    private final CommonMessageTask<SendMessageBatchRequestEntry, SendMessageBatchResultEntry, SendMessageResult,
            SendMessageBatchRequest> commonTask = new CommonMessageTask<>(SendMessageBatchRequestEntry::setId,
            SendMessageBatchResultEntry::getId, SendMessageTask.getMessageResult, sendBatchRequestMock);


    @Before
    public void setup() {
        list.add(new BatchRequestEntry<>(entry, singleSubject));
        when(sendBatchRequestMock.apply(any())).thenReturn(Single.just(BATCH_RESULT_SUCCESS));
    }


    @Test
    public void testSuccess() {
        commonTask.run(QUEUE_URL, list);
        singleSubject.test().assertComplete();
    }

    @Test
    public void testClientError() {
        when(sendBatchRequestMock.apply(any())).thenReturn(Single.just(BATCH_RESULT_CLIENT_ERROR));
        commonTask.run(QUEUE_URL, list);
        singleSubject.test().assertError(throwable ->
                ((AmazonServiceException)throwable).getErrorType() == AmazonServiceException.ErrorType.Client);
    }

    @Test
    public void testServerError() {
        when(sendBatchRequestMock.apply(any())).thenReturn(Single.just(BATCH_RESULT_SERVER_ERROR));
        commonTask.run(QUEUE_URL, list);
        singleSubject.test().assertError(throwable ->
                ((AmazonServiceException)throwable).getErrorType() == AmazonServiceException.ErrorType.Service);
    }

    @Test
    public void testRequestFailed() {
        Exception exception = new RuntimeException();
        when(sendBatchRequestMock.apply(any())).thenReturn(Single.error(exception));
        commonTask.run(QUEUE_URL, list);
        singleSubject.test().assertError(exception);
    }
}
