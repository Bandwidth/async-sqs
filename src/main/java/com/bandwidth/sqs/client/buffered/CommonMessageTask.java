package com.bandwidth.sqs.client.buffered;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.bandwidth.sqs.client.BaseSqsAsyncIoClient;
import com.bandwidth.sqs.client.BatchRequestEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.reactivex.Single;

public class CommonMessageTask<BatchRequestEntryT, BatchResultEntryT, MessageResultT, BatchRequestT extends
        AmazonWebServiceRequest>
        implements KeyedTaskBuffer.Task<String, BatchRequestEntry<BatchRequestEntryT, MessageResultT>> {

    private final BiConsumer<BatchRequestEntryT, String> setRequestEntryId;
    private final Function<BatchResultEntryT, String> getResultEntryId;
    private final Function<BatchResultEntryT, MessageResultT> getMessageResult;
    private final Function<BatchRequest<BatchRequestEntryT>, Single<BatchResult<BatchResultEntryT>>> sendBatchRequest;

    public CommonMessageTask(BiConsumer<BatchRequestEntryT, String> setEntryId,
            Function<BatchResultEntryT, String> getEntryId,
            Function<BatchResultEntryT, MessageResultT> getMessageResult,
            Function<BatchRequest<BatchRequestEntryT>, Single<BatchResult<BatchResultEntryT>>> sendBatchRequest) {
        this.setRequestEntryId = setEntryId;
        this.getResultEntryId = getEntryId;
        this.getMessageResult = getMessageResult;
        this.sendBatchRequest = sendBatchRequest;

    }

    @Override
    public void run(String queueUrl, List<BatchRequestEntry<BatchRequestEntryT, MessageResultT>> requests) {
        int requestEntryId = 0;
        List<BatchRequestEntryT> batchRequestEntries = new ArrayList<>();
        Map<String, BatchRequestEntry<BatchRequestEntryT, MessageResultT>> requestMap = new
                HashMap<>();

        for (BatchRequestEntry<BatchRequestEntryT, MessageResultT> request : requests) {
            String currentId = Integer.toString(requestEntryId++);
            setRequestEntryId.accept(request.getRequestEntry(), currentId);
            batchRequestEntries.add(request.getRequestEntry());
            requestMap.put(currentId, request);
        }

        sendBatchRequest.apply(new BatchRequest<>(queueUrl, batchRequestEntries))
                .subscribe((batchResult) -> {
                    batchResult.getSuccessResults().forEach((batchResultEntry) -> {
                        BatchRequestEntry<BatchRequestEntryT, MessageResultT> asyncRequest =
                                requestMap.get(getResultEntryId.apply(batchResultEntry));
                        asyncRequest.onSuccess(getMessageResult.apply(batchResultEntry));
                    });
                    batchResult.getErrorResults().forEach((entry) -> {
                        final BatchRequestEntry<BatchRequestEntryT, MessageResultT>
                                asyncRequest = requestMap.get(entry.getId());
                        AmazonServiceException exception = new AmazonServiceException(entry.getMessage());
                        exception.setErrorCode(entry.getCode());
                        exception.setErrorType(entry.isSenderFault()
                                ? AmazonServiceException.ErrorType.Client : AmazonServiceException.ErrorType.Service);
                        exception.setServiceName(BaseSqsAsyncIoClient.FULL_SERVICE_NAME);
                        asyncRequest.onError(exception);
                    });
                }, (awsServiceError) -> {
                        requestMap.values().forEach(request -> request.onError(awsServiceError));
                    });
    }
}