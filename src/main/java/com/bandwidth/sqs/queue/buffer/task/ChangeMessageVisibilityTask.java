package com.bandwidth.sqs.queue.buffer.task;

import com.bandwidth.sqs.action.ChangeMessageVisibilityBatchAction;
import com.bandwidth.sqs.queue.entry.ChangeMessageVisibilityEntry;
import com.bandwidth.sqs.action.sender.SqsRequestSender;

import java.util.Map;
import java.util.Optional;

public class ChangeMessageVisibilityTask implements Task<String, ChangeMessageVisibilityEntry> {

    private final SqsRequestSender requestSender;

    public ChangeMessageVisibilityTask(SqsRequestSender requestSender) {
        this.requestSender = requestSender;
    }

    @Override
    public void run(String queueUrl, Map<String, ChangeMessageVisibilityEntry> entryMap) {
        ChangeMessageVisibilityBatchAction action = new ChangeMessageVisibilityBatchAction(queueUrl, entryMap);

        requestSender.sendRequest(action).subscribe((result) -> {
            result.getSuccessful().forEach((successfulResult) -> {
                Optional.ofNullable(entryMap.get(successfulResult.getId()))
                        .ifPresent((entry) -> entry.getResultSubject().onComplete());
            });
            result.getFailed().forEach((errorResult) -> {
                Optional.ofNullable(entryMap.get(errorResult.getId()))
                        .ifPresent((entry) -> {
                            entry.getResultSubject().onError(new RuntimeException(errorResult.getMessage()));
                        });
            });
        }, (err) -> {//the entire batch operation failed
            entryMap.values().forEach((entry) -> entry.getResultSubject().onError(err));
        });
    }
}
