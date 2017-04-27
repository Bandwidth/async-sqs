package com.bandwidth.sqs.queue.buffer.task;

import com.bandwidth.sqs.actions.SendMessageBatchAction;
import com.bandwidth.sqs.queue.buffer.entry.SendMessageEntry;
import com.bandwidth.sqs.queue.buffer.task_buffer.Task;
import com.bandwidth.sqs.request_sender.SqsRequestSender;

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
    public void run(String queueUrl, List<SendMessageEntry> entries) {
        Map<String, SendMessageEntry> entryMap = new HashMap<>();
        IntStream.range(0, entries.size()).forEach((id) -> {
            entryMap.put(Integer.toString(id), entries.get(id));
        });

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
            entries.forEach((entry) -> entry.getResultSubject().onError(err));
        });
    }
}