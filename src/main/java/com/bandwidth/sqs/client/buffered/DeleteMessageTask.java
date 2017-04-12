package com.bandwidth.sqs.client.buffered;

import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.bandwidth.sqs.client.SqsAsyncIoClient;

import java.util.function.Function;

import io.reactivex.Single;

public class DeleteMessageTask extends CommonMessageTask<DeleteMessageBatchRequestEntry,
        DeleteMessageBatchResultEntry, DeleteMessageResult, DeleteMessageBatchRequest> {

    public static final Function<DeleteMessageBatchResultEntry, DeleteMessageResult>
            getMessageResult = (notUsed) -> new DeleteMessageResult();

    public DeleteMessageTask(SqsAsyncIoClient sqsClient) {
        super(DeleteMessageBatchRequestEntry::setId, DeleteMessageBatchResultEntry::getId,
                getMessageResult, new SendBatchRequestTask(sqsClient));
    }

    public static final class SendBatchRequestTask implements Function<BatchRequest<DeleteMessageBatchRequestEntry>,
            Single<BatchResult<DeleteMessageBatchResultEntry>>> {

        private final SqsAsyncIoClient sqsClient;

        public SendBatchRequestTask(SqsAsyncIoClient sqsClient) {
            this.sqsClient = sqsClient;
        }

        @Override
        public Single<BatchResult<DeleteMessageBatchResultEntry>> apply(BatchRequest<DeleteMessageBatchRequestEntry>
                request) {
            DeleteMessageBatchRequest deleteRequest = new DeleteMessageBatchRequest()
                    .withQueueUrl(request.getQueueUrl())
                    .withEntries(request.getBatchRequestEntries());

            return sqsClient.deleteMessageBatch(deleteRequest)
                    .map((result) -> new BatchResult<>(result.getSuccessful(), result.getFailed()));
        }
    }
}