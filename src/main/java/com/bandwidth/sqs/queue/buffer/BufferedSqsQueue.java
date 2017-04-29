package com.bandwidth.sqs.queue.buffer;

import com.google.common.annotations.VisibleForTesting;

import com.bandwidth.sqs.actions.GetQueueAttributesAction;
import com.bandwidth.sqs.actions.ReceiveMessagesAction;
import com.bandwidth.sqs.actions.SetQueueAttributesAction;
import com.bandwidth.sqs.queue.ImmutableSqsMessage;
import com.bandwidth.sqs.queue.SqsMessage;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueAttributes;
import com.bandwidth.sqs.queue.SqsQueueClientConfig;
import com.bandwidth.sqs.queue.buffer.task.ChangeMessageVisibilityTask;
import com.bandwidth.sqs.queue.buffer.task.DeleteMessageTask;
import com.bandwidth.sqs.queue.buffer.task.SendMessageTask;
import com.bandwidth.sqs.queue.buffer.KeyedTaskBuffer;
import com.bandwidth.sqs.actions.sender.SqsRequestSender;
import com.bandwidth.sqs.queue.entry.ChangeMessageVisibilityEntry;
import com.bandwidth.sqs.queue.entry.DeleteMessageEntry;
import com.bandwidth.sqs.queue.entry.SendMessageEntry;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.reactivex.Completable;
import io.reactivex.Single;

public class BufferedSqsQueue implements SqsQueue<String> {
    public static final int MAX_BUFFER_SIZE = 10;

    private final String queueUrl;
    private final SqsRequestSender requestSender;
    private final AtomicReference<Single<SqsQueueAttributes>> attributes = new AtomicReference<>();

    private KeyedTaskBuffer<String, SendMessageEntry> sendMessageTaskBuffer;
    private KeyedTaskBuffer<String, DeleteMessageEntry> deleteMessageTaskBuffer;
    private KeyedTaskBuffer<String, ChangeMessageVisibilityEntry> changeMessageVisibilityTaskBuffer;

    public BufferedSqsQueue(String queueUrl, SqsRequestSender requestSender, SqsQueueClientConfig clientConfig,
            Optional<SqsQueueAttributes> queueAttributes) {
        this.queueUrl = queueUrl;
        this.requestSender = requestSender;

        this.attributes.set(queueAttributes.map(Single::just).orElse(Single.defer(() -> {
            GetQueueAttributesAction action = new GetQueueAttributesAction(queueUrl);
            return requestSender.sendRequest(action).map(getQueueAttributesResult -> SqsQueueAttributes.builder()
                    .fromStringMap(getQueueAttributesResult.getAttributes())
                    .build()
            );
        })).cache());

        Duration bufferDelay = clientConfig.getBufferDelay();
        this.sendMessageTaskBuffer =
                new KeyedTaskBuffer<>(MAX_BUFFER_SIZE, bufferDelay, new SendMessageTask(requestSender));
        this.deleteMessageTaskBuffer =
                new KeyedTaskBuffer<>(MAX_BUFFER_SIZE, bufferDelay, new DeleteMessageTask(requestSender));
        this.changeMessageVisibilityTaskBuffer =
                new KeyedTaskBuffer<>(MAX_BUFFER_SIZE, bufferDelay, new ChangeMessageVisibilityTask(requestSender));
    }

    @Override
    public String getQueueUrl() {
        return queueUrl;
    }

    @Override
    public Single<SqsQueueAttributes> getAttributes() {
        return attributes.get();
    }

    @Override
    public Single<String> publishMessage(String message, Optional<Duration> maybeDelay) {
        SendMessageEntry entry = SendMessageEntry.builder()
                .body(message)
                .delay(maybeDelay)
                .build();
        sendMessageTaskBuffer.addData(queueUrl, entry);
        return entry.getResultSubject();
    }

    @Override
    public Completable deleteMessage(String receiptHandle) {
        DeleteMessageEntry entry = DeleteMessageEntry.builder()
                .receiptHandle(receiptHandle)
                .build();
        deleteMessageTaskBuffer.addData(queueUrl, entry);
        return entry.getResultSubject();
    }

    @Override
    public Completable changeMessageVisibility(String receiptHandle, Duration newVisibility) {
        ChangeMessageVisibilityEntry entry = ChangeMessageVisibilityEntry.builder()
                .receiptHandle(receiptHandle)
                .newVisibilityTimeout(newVisibility)
                .build();
        changeMessageVisibilityTaskBuffer.addData(queueUrl, entry);
        return entry.getResultSubject();
    }

    @Override
    public Completable setAttributes(SqsQueueAttributes newAttributes) {
        SetQueueAttributesAction action = new SetQueueAttributesAction(queueUrl, newAttributes);
        Completable setAttributesComplete = requestSender.sendRequest(action)
                .toCompletable();
        setAttributesComplete.subscribe(() -> attributes.set(Single.just(newAttributes)));
        return setAttributesComplete;
    }

    @Override
    public Single<List<SqsMessage<String>>> receiveMessages(int maxMessages, Optional<Duration> waitTime,
            Optional<Duration> visibilityTimeout) {
        ReceiveMessagesAction action = new ReceiveMessagesAction(queueUrl, maxMessages, waitTime, visibilityTimeout);
        return requestSender.sendRequest(action).map(receiveMessageResult -> receiveMessageResult.getMessages()
                .stream().map((msg) -> ImmutableSqsMessage.<String>builder()
                        .body(msg.getBody())
                        .receiptHandle(msg.getReceiptHandle())
                        .id(msg.getMessageId())
                        .build()
                ).collect(Collectors.toList())
        );
    }

    @VisibleForTesting
    void setSendMessageTaskBuffer(KeyedTaskBuffer<String, SendMessageEntry> buffer) {
        this.sendMessageTaskBuffer = buffer;
    }

    @VisibleForTesting
    void setDeleteMessageTaskBuffer(KeyedTaskBuffer<String, DeleteMessageEntry> buffer) {
        this.deleteMessageTaskBuffer = buffer;
    }

    @VisibleForTesting
    void setChangeMessageVisibilityTaskBuffer(KeyedTaskBuffer<String, ChangeMessageVisibilityEntry> buffer) {
        this.changeMessageVisibilityTaskBuffer = buffer;
    }

}