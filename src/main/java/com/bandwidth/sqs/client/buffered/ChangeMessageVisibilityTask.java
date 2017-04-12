package com.bandwidth.sqs.client.buffered;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResultEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.bandwidth.sqs.client.SqsAsyncIoClient;

import java.util.function.Function;

import io.reactivex.Single;

public class ChangeMessageVisibilityTask extends CommonMessageTask<ChangeMessageVisibilityBatchRequestEntry,
        ChangeMessageVisibilityBatchResultEntry,
        ChangeMessageVisibilityResult, ChangeMessageVisibilityBatchRequest> {

    public static final Function<ChangeMessageVisibilityBatchResultEntry, ChangeMessageVisibilityResult>
            getMessageResult = (notUsed) -> new ChangeMessageVisibilityResult();

    public ChangeMessageVisibilityTask(SqsAsyncIoClient sqsClient) {
        super(ChangeMessageVisibilityBatchRequestEntry::setId, ChangeMessageVisibilityBatchResultEntry::getId,
                getMessageResult,
                new SendBatchRequestTask(sqsClient)
        );
    }

    public static final class SendBatchRequestTask implements
            Function<BatchRequest<ChangeMessageVisibilityBatchRequestEntry>,
                    Single<BatchResult<ChangeMessageVisibilityBatchResultEntry>>> {
        private final SqsAsyncIoClient sqsClient;

        public SendBatchRequestTask(SqsAsyncIoClient sqsClient) {
            this.sqsClient = sqsClient;
        }


        @Override
        public Single<BatchResult<ChangeMessageVisibilityBatchResultEntry>>
                apply(BatchRequest<ChangeMessageVisibilityBatchRequestEntry> batchRequest) {
            ChangeMessageVisibilityBatchRequest changeVisibilityRequest = new ChangeMessageVisibilityBatchRequest()
                    .withQueueUrl(batchRequest.getQueueUrl())
                    .withEntries(batchRequest.getBatchRequestEntries());

            return sqsClient.changeMessageVisibilityBatch(changeVisibilityRequest)
                    .map((result) -> new BatchResult<>(result.getSuccessful(), result.getFailed()));
        }
    }
}