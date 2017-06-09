package com.bandwidth.sqs.consumer;

import com.bandwidth.sqs.queue.SqsMessage;

import java.util.function.Consumer;


public class PriorityTask<T> implements Consumer<SqsMessage<T>> {

    private final Consumer<SqsMessage<T>> runnable;
    private final int priority;
    private final SqsConsumer<T> consumer;//the consumer that owns this task

    public PriorityTask(int priority, SqsConsumer<T> consumer, Consumer<SqsMessage<T>> runnable) {
        this.priority = priority;
        this.runnable = runnable;
        this.consumer = consumer;
    }

    public int getPriority() {
        return priority;
    }

    public SqsMessage<T> getNextMessage() {
        return consumer.getNextMessage();
    }

    public void updateConsumer() {
        consumer.update();
    }

    @Override
    public void accept(SqsMessage<T> sqsMessage) {
        runnable.accept(sqsMessage);
    }
}
