package com.bandwidth.sqs.queue.buffer.task;

import com.bandwidth.sqs.actions.ChangeMessageVisibilityBatchAction;
import com.bandwidth.sqs.queue.buffer.entry.ChangeMessageVisibilityEntry;
import com.bandwidth.sqs.queue.buffer.task_buffer.Task;
import com.bandwidth.sqs.request_sender.SqsRequestSender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

public class ChangeMessageVisibilityTask implements Task<String, ChangeMessageVisibilityEntry> {

    private final SqsRequestSender requestSender;

    public ChangeMessageVisibilityTask(SqsRequestSender requestSender){
        this.requestSender = requestSender;
    }

    @Override
    public void run(String queueUrl, List<ChangeMessageVisibilityEntry> entries) {
        Map<String, ChangeMessageVisibilityEntry> entryMap = new HashMap<>();
        IntStream.range(0, entries.size()).forEach((id) -> {
            entryMap.put(Integer.toString(id), entries.get(id));
        });

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
            entries.forEach((entry) -> entry.getResultSubject().onError(err));
        });
    }
}
