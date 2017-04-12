package com.bandwidth.sqs.client.buffered;

import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.bandwidth.sqs.client.SqsAsyncIoClient;

import java.util.function.Function;

import io.reactivex.Single;

public class SendMessageTask extends CommonMessageTask<SendMessageBatchRequestEntry, SendMessageBatchResultEntry,
        SendMessageResult, SendMessageBatchRequest> {

    public static final Function<SendMessageBatchResultEntry, SendMessageResult>
            getMessageResult = (entry) -> new SendMessageResult()
            .withMessageId(entry.getMessageId())
            .withMD5OfMessageAttributes(entry.getMD5OfMessageAttributes())
            .withMD5OfMessageBody(entry.getMD5OfMessageBody());

    public SendMessageTask(SqsAsyncIoClient sqsClient) {
        super(SendMessageBatchRequestEntry::setId, SendMessageBatchResultEntry::getId, getMessageResult,
                new SendBatchRequestTask(sqsClient)
        );
    }

    public static final class SendBatchRequestTask implements Function<BatchRequest<SendMessageBatchRequestEntry>,
            Single<BatchResult<SendMessageBatchResultEntry>>> {
        private final SqsAsyncIoClient sqsClient;

        public SendBatchRequestTask(SqsAsyncIoClient sqsClient) {
            this.sqsClient = sqsClient;
        }


        @Override
        public Single<BatchResult<SendMessageBatchResultEntry>> apply(BatchRequest<SendMessageBatchRequestEntry>
                batchRequest) {

            SendMessageBatchRequest sendRequest = new SendMessageBatchRequest()
                    .withQueueUrl(batchRequest.getQueueUrl())
                    .withEntries(batchRequest.getBatchRequestEntries());

            return sqsClient.sendMessageBatch(sendRequest)
                    .map((result) -> new BatchResult<>(result.getSuccessful(), result.getFailed()));
        }
    }
}