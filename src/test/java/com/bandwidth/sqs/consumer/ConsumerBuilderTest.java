package com.bandwidth.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.model.Message;
import com.bandwidth.sqs.consumer.handler.ConsumerHandler;
import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;
import com.bandwidth.sqs.consumer.strategy.backoff.NullBackoffStrategy;
import com.bandwidth.sqs.consumer.strategy.expiration.ExpirationStrategy;
import com.bandwidth.sqs.consumer.strategy.expiration.NeverExpiresStrategy;
import com.bandwidth.sqs.queue.SqsQueue;

import org.junit.Before;
import org.junit.Test;

import io.reactivex.Observable;

@SuppressWarnings("unchecked")
public class ConsumerBuilderTest {

    private static final int BUFFER_SIZE = ConsumerBuilder.DEFAULT_BUFFER_SIZE + 1;
    private static final int NUM_PERMITS = ConsumerBuilder.DEFAULT_NUM_PERMITS + 1;

    private final SqsQueue<String> sqsQueueMock = mock(SqsQueue.class);
    private final ConsumerManager consumerManagerMock = mock(ConsumerManager.class);
    private final ConsumerHandler<String> consumerHandlerMock = mock(ConsumerHandler.class);
    private final BackoffStrategy backoffStrategy = new NullBackoffStrategy();
    private final ExpirationStrategy expirationStrategy = new NeverExpiresStrategy();

    private final ConsumerBuilder builder = new ConsumerBuilder(consumerManagerMock, sqsQueueMock, consumerHandlerMock);

    @Before
    public void setup() {
        when(consumerHandlerMock.getPermitChangeRequests()).thenReturn(Observable.never());
    }

    @Test
    public void testBuilder() {
        Consumer consumer = builder
                .withBackoffStrategy(backoffStrategy)
                .withBufferSize(BUFFER_SIZE)
                .withNumPermits(NUM_PERMITS)
                .withExpirationStrategy(expirationStrategy)
                .build();

        assertThat(consumer.getBackoffStrategy()).isEqualTo(backoffStrategy);
        assertThat(consumer.getBufferSize()).isEqualTo(BUFFER_SIZE);
        assertThat(consumer.getNumPermits()).isEqualTo(NUM_PERMITS);
        assertThat(consumer.getExpirationStrategy()).isEqualTo(expirationStrategy);
    }
}
