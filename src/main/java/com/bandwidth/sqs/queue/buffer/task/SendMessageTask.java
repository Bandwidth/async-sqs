package com.bandwidth.sqs.queue.buffer.task;

import com.bandwidth.sqs.actions.SendMessageBatchAction;
import com.bandwidth.sqs.queue.entry.SendMessageEntry;
import com.bandwidth.sqs.actions.sender.SqsRequestSender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

public class SendMessageTask implements Task<String, SendMessageEntry> {

    private final SqsRequestSender requestSender;

    public SendMessageTask(SqsRequestSender requestSender) {
        this.requestSender = requestSender;
    }

    @Override
    public void run(String queueUrl, Map<String, SendMessageEntry> entryMap) {
        SendMessageBatchAction action = new SendMessageBatchAction(queueUrl, entryMap);

        requestSender.sendRequest(action).subscribe((result) -> {
            result.getSuccessful().forEach((successfulResult) -> {
                Optional.ofNullable(entryMap.get(successfulResult.getId()))
                        .ifPresent((entry) -> entry.getResultSubject().onSuccess(successfulResult.getMessageId()));
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