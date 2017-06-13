package com.bandwidth.sqs.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.bandwidth.sqs.queue.SqsMessage;

import org.junit.Test;

import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public class PriorityTaskTest {

    private static final int PRIORITY = 42;

    private final SqsConsumer consumerMock = mock(SqsConsumer.class);
    private final Consumer consumerFunctionMock = mock(Consumer.class);
    private final SqsMessage sqsMessageMock = mock(SqsMessage.class);

    private final PriorityTask priorityTask = new PriorityTask(PRIORITY, consumerMock, consumerFunctionMock);

    @Test
    public void testGetNextMessage() {
        priorityTask.getNextMessage();
        verify(consumerMock).getNextMessage();
    }

    @Test
    public void testUpdateConsumer() {
        priorityTask.updateConsumer();
        verify(consumerMock).update();
    }

    @Test
    public void testAccept() {
        Consumer<SqsMessage> priorityTaskAsConsumer = priorityTask;
        priorityTaskAsConsumer.accept(sqsMessageMock);
        verify(consumerFunctionMock).accept(sqsMessageMock);
    }
}
