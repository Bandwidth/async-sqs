package com.bandwidth.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.bandwidth.sqs.client.SqsAsyncIoClient;
import com.bandwidth.sqs.consumer.Consumer.LoadBalanceRequestUpdater;
import com.bandwidth.sqs.consumer.Consumer.ReceiveMessageHandler;
import com.bandwidth.sqs.consumer.Consumer.RequestType;
import com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger;
import com.bandwidth.sqs.consumer.strategy.expiration.ExpirationStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy.Action;
import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;
import com.bandwidth.sqs.consumer.handler.ConsumerHandler;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;

import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.subjects.SingleSubject;

@SuppressWarnings("unchecked")
public class ConsumerTest {

    private static final String QUEUE_URL = "http://www.domain/path";
    private static final int NUM_PERMITS = 2;
    private static final int NO_PERMITS = 0;
    private static final int MAX_QUEUE_SIZE = 20;
    private static final int MAX_QUEUE_SIZE_1 = 1;
    private static final int MESSAGE_COUNT = 7;
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(10);

    private final ArrayDeque<TimedMessage> messageBufferEmpty = spy(new ArrayDeque<TimedMessage>());
    private final ArrayDeque<TimedMessage> messageBufferSmall = spy(new ArrayDeque<TimedMessage>());
    private final ArrayDeque<TimedMessage> messageBufferFull = spy(new ArrayDeque<TimedMessage>());
    private final BackoffStrategy backoffStrategyMock = mock(BackoffStrategy.class);
    private final ConsumerManager consumerManagerMock = mock(ConsumerManager.class);
    private final ConsumerHandler<Message> consumerHandlerMock = mock(ConsumerHandler.class);
    private final SqsAsyncIoClient sqsClientMock = mock(SqsAsyncIoClient.class);
    private final Message messageMock = mock(Message.class);
    private final LoadBalanceStrategy loadBalanceStrategyMock = mock(LoadBalanceStrategy.class);
    private final ExpirationStrategy expirationStrategyMock = mock(ExpirationStrategy.class);

    @Captor
    private ArgumentCaptor<MessageAcknowledger> acknowledgerCaptor;

    @Captor
    private ArgumentCaptor<AsyncHandler> asyncHandlerCaptor;

    @Captor
    private ArgumentCaptor<ReceiveMessageHandler> receiveMessageHandlerCaptor;

    private Consumer consumer;

    public ConsumerTest() {
        when(consumerHandlerMock.getPermitChangeRequests()).thenReturn(Observable.never());
        when(backoffStrategyMock.getWindowSize()).thenReturn(WINDOW_SIZE);
        when(consumerManagerMock.getSqsClient()).thenReturn(sqsClientMock);

        consumer = new ConsumerBuilder(consumerManagerMock, QUEUE_URL, consumerHandlerMock)
                .withNumPermits(NUM_PERMITS)
                .withBufferSize(MAX_QUEUE_SIZE)
                .withBackoffStrategy(backoffStrategyMock)
                .withExpirationStrategy(expirationStrategyMock)
                .build();

        consumer.setLoadBalanceStrategy(loadBalanceStrategyMock);

        messageBufferSmall.push(TimedMessage.builder().message(messageMock).build());
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            messageBufferFull.push(TimedMessage.builder().message(messageMock).build());
        }
        when(backoffStrategyMock.getDelayTime(anyDouble())).thenReturn(Duration.ZERO);
        when(sqsClientMock.receiveMessage(any())).thenReturn(Single.never());
        when(sqsClientMock.deleteMessage(any())).thenReturn(Single.never());

    }

    @Test
    public void testStartLongPollingRequest() {
        consumer.start();
        ArgumentCaptor<ReceiveMessageRequest> requestCaptor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        //the first request sent will be the long-polling request
        verify(sqsClientMock).receiveMessage(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getWaitTimeSeconds()).isEqualTo(Consumer.MAX_WAIT_TIME_SECONDS);
    }

    @Test
    public void testSetNumPermits() {
        int numPermits = 1234;
        consumer.setNumPermits(numPermits);
        assertThat(consumer.getNumPermits()).isEqualTo(numPermits);
    }

    @Test
    public void testStartFirstLoadBalancedRequest() {
        when(consumerManagerMock.getAllocatedInFlightRequestsCount(consumer)).thenReturn(1);
        consumer.start();//long-polling request will be started first, then load balanced request 2nd
        ArgumentCaptor<ReceiveMessageRequest> requestCaptor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        verify(sqsClientMock, times(2)).receiveMessage(requestCaptor.capture());
        assertThat(requestCaptor.getAllValues().get(1).getWaitTimeSeconds()).isEqualTo(
                Consumer.LOAD_BALANCED_REQUEST_WAIT_TIME_SECONDS);
    }

    @Test
    public void testUpdateWhileAllRequestsStarted() {
        consumer.start();//long-polling request will be started
        consumer.update();//nothing should be started
        ArgumentCaptor<ReceiveMessageRequest> requestCaptor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        verify(sqsClientMock).receiveMessage(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getWaitTimeSeconds()).isEqualTo(Consumer.MAX_WAIT_TIME_SECONDS);
    }

    @Test
    public void testStartNewRequestBufferFull() {
        consumer = new ConsumerBuilder(consumerManagerMock, QUEUE_URL, consumerHandlerMock)
                .withNumPermits(NO_PERMITS)
                .withBufferSize(MAX_QUEUE_SIZE_1)
                .withBackoffStrategy(backoffStrategyMock)
                .withExpirationStrategy(expirationStrategyMock)
                .build();
        consumer.setMessageBuffer(messageBufferSmall);
        consumer.start();//buffer is full, no requests will be started
        verify(sqsClientMock, never()).receiveMessage(any());
    }

    @Test
    public void testQueueForProcessing() {
        consumer.setMessageBuffer(messageBufferSmall);
        consumer.start();//will be queued for processing
        consumer.update();//already queued, won't queue again
        verify(consumerManagerMock).queueTask(any());
    }

    @Test
    public void testBackoffDelay() {
        consumer = new ConsumerBuilder(consumerManagerMock, QUEUE_URL, consumerHandlerMock)
                .withNumPermits(NUM_PERMITS)
                .withBufferSize(MAX_QUEUE_SIZE_1)
                .withBackoffStrategy(backoffStrategyMock)
                .withExpirationStrategy(expirationStrategyMock)
                .build();

        consumer.setMessageBuffer(messageBufferSmall);
        when(backoffStrategyMock.getDelayTime(anyDouble())).thenReturn(Duration.ofDays(999999));
        consumer.checkIfBackoffDelayNeeded();
        consumer.start();//backoffDelay prevents consumer from being queued
        verify(consumerManagerMock, never()).queueTask(any());
    }

    @Test
    public void testNegativeBackoffDelay() {
        when(backoffStrategyMock.getDelayTime(anyInt())).thenReturn(Duration.ofDays(-1));
        consumer.checkIfBackoffDelayNeeded();
        verify(consumerManagerMock, never()).queueTask(any());
    }

    @Test
    public void testTimerTaskUpdate() {
        Consumer consumerMock = mock(Consumer.class);
        Consumer.UpdateTimerTask task = consumerMock.new UpdateTimerTask();
        task.run();
        verify(consumerMock).update();
    }

    @Test
    public void testProcessNextMessage() {
        consumer.setMessageBuffer(messageBufferSmall);
        consumer.processNextMessage();
        verify(consumerHandlerMock).handleMessage(eq(messageMock), any());
    }

    @Test
    public void testProcessNextMessageExpired() {
        when(expirationStrategyMock.isExpired(any())).thenReturn(true);
        consumer.setMessageBuffer(messageBufferSmall);
        consumer.processNextMessage();
        verify(consumerHandlerMock, never()).handleMessage(eq(messageMock), any());
    }

    @Test
    public void testLoadBalanceRequestUpdater() {
        consumer.setMessageBuffer(messageBufferSmall);
        LoadBalanceRequestUpdater updater = consumer.new LoadBalanceRequestUpdater(MESSAGE_COUNT);
        updater.getAction(0);
        verify(loadBalanceStrategyMock).onReceiveSuccess(MESSAGE_COUNT);
    }

    @Test
    public void testLoadBalanceRequestUpdaterBufferFull() {
        consumer.setMessageBuffer(messageBufferFull);
        LoadBalanceRequestUpdater updater = consumer.new LoadBalanceRequestUpdater(MESSAGE_COUNT);
        assertThat(updater.getAction(0)).isEqualTo(Action.Decrease);
    }

    @Test
    public void testReceiveMessageHandlerUpdateLoadBalanceRequests() {
        ReceiveMessageHandler handler = consumer.new ReceiveMessageHandler(RequestType.LoadBalanced);
        handler.updateLoadBalanceRequests(MESSAGE_COUNT);
        verify(consumerManagerMock).updateAllocatedInFlightRequests(eq(consumer), any());
    }

    @Test
    public void testReceiveMessageHandlerOnError() {
        ReceiveMessageHandler handler = consumer.new ReceiveMessageHandler(RequestType.LoadBalanced);
        ReceiveMessageHandler handlerSpy = spy(handler);
        handlerSpy.onError(new NullPointerException());
        verify(handlerSpy).always();
    }

    @Test
    public void testReceiveMessageHandlerOnSuccess() {
        Consumer consumerSpy = spy(consumer);
        SingleObserver<ReceiveMessageResult> handler =
                spy(consumerSpy.new ReceiveMessageHandler(RequestType.LongPolling));
        ReceiveMessageResult result = new ReceiveMessageResult().withMessages(Collections.singletonList(messageMock));
        handler.onSuccess(result);

        verify(consumerSpy, times(2)).update();
    }

    @Test
    public void testReceiveMessageHandlerOnSuccessNoMessages() {
        consumer.setMessageBuffer(messageBufferEmpty);
        ReceiveMessageHandler handler = consumer.new ReceiveMessageHandler(RequestType.LongPolling);
        handler.onSuccess(new ReceiveMessageResult().withMessages(Collections.emptyList()));

        assertThat(messageBufferEmpty.size()).isEqualTo(0);
    }

    @Test
    public void testDoNotReceiveMessageWhenInShutdown() {
        consumer.shutdown();
        consumer.update();
        verifyZeroInteractions(sqsClientMock);
    }

    @Test
    public void testShutdownWithoutStarting() {
        consumer.setMessageBuffer(messageBufferEmpty);
        consumer.shutdown();
        assertThat(consumer.isShutdown()).isTrue();
    }

    @Test
    public void testIsNotTerminatedWhenIsNotShutdown() {
        consumer.setMessageBuffer(messageBufferEmpty);
        assertThat(consumer.isShutdown()).isFalse();
    }

    @Test
    public void testIsNotTerminatedWhenBufferIsNotEmpty() {
        consumer.setMessageBuffer(messageBufferSmall);
        consumer.shutdownAsync().test().assertNotComplete();
    }

    @Test
    public void testIsNotTerminatedWhenProcessingMessage() {
        consumer.setMessageBuffer(messageBufferSmall);
        consumer.processNextMessage();
        consumer.shutdownAsync().test().assertNotComplete();
    }

    @Test
    public void testIsNotTerminatedWhenWaitingLongPollRequest() throws Exception {
        consumer.setMessageBuffer(messageBufferEmpty);
        consumer.update();
        consumer.shutdownAsync().test().assertNotComplete();
    }

    @Test
    public void testIsNotTerminatedWhenWaitingLoadBalancedRequest() throws Exception {
        consumer.setMessageBuffer(messageBufferFull);
        when(consumerManagerMock.getAllocatedInFlightRequestsCount(any())).thenReturn(1);
        consumer.update();
        consumer.setMessageBuffer(messageBufferEmpty);
        consumer.shutdownAsync().test().assertNotComplete();
    }

    @Test
    public void testShutdownWithPendingPermits() {
        SingleSubject<ReceiveMessageResult> singleSubject = SingleSubject.create();

        when(sqsClientMock.deleteMessage(any())).thenReturn(Single.just(mock(DeleteMessageResult.class)));
        when(sqsClientMock.receiveMessage(any())).thenReturn(singleSubject);

        consumer.setMessageBuffer(messageBufferSmall);
        consumer.processNextMessage();

        //handler does not ack here, so permits will be pending forever

        Completable shutdownCompletable = consumer.shutdownAsync();
        singleSubject.onSuccess(new ReceiveMessageResult());

        shutdownCompletable.test().assertNotComplete();
    }

    @Test
    public void testHandlerDeleteAndShutdown() {
        SingleSubject<ReceiveMessageResult> singleSubject = SingleSubject.create();

        when(sqsClientMock.deleteMessage(any())).thenReturn(Single.just(mock(DeleteMessageResult.class)));
        when(sqsClientMock.receiveMessage(any())).thenReturn(singleSubject);

        doAnswer((invocation -> {
            ((MessageAcknowledger)invocation.getArgument(1)).delete();
            return null;
        })).when(consumerHandlerMock).handleMessage(any(), any());

        consumer.setMessageBuffer(messageBufferSmall);
        consumer.processNextMessage();
        Completable shutdownCompletable = consumer.shutdownAsync();
        singleSubject.onSuccess(new ReceiveMessageResult());

        shutdownCompletable.test().assertComplete();
        verify(sqsClientMock).deleteMessage(any());
    }

    @Test
    public void testHandlerIgnoreAndShutdown() {
        SingleSubject<ReceiveMessageResult> singleSubject = SingleSubject.create();

        when(sqsClientMock.deleteMessage(any())).thenReturn(Single.just(mock(DeleteMessageResult.class)));
        when(sqsClientMock.receiveMessage(any())).thenReturn(singleSubject);

        doAnswer((invocation -> {
            ((MessageAcknowledger)invocation.getArgument(1)).ignore();
            return null;
        })).when(consumerHandlerMock).handleMessage(any(), any());

        consumer.setMessageBuffer(messageBufferSmall);
        consumer.processNextMessage();
        Completable shutdownCompletable = consumer.shutdownAsync();
        singleSubject.onSuccess(new ReceiveMessageResult());

        shutdownCompletable.test().assertComplete();
        verify(sqsClientMock, never()).deleteMessage(any());
    }

    @Test
    public void testRetryAck() {
        ConsumerHandler<Message> handlerSpy = spy(new RetryingHandler());
        consumer = new ConsumerBuilder(consumerManagerMock, QUEUE_URL, handlerSpy)
                .withNumPermits(NO_PERMITS)
                .withBufferSize(MAX_QUEUE_SIZE_1)
                .withBackoffStrategy(backoffStrategyMock)
                .withExpirationStrategy(expirationStrategyMock)
                .build();
        consumer.setMessageBuffer(messageBufferSmall);
        consumer.processNextMessage();
        consumer.processNextMessage();
        verify(handlerSpy, times(2)).handleMessage(eq(messageMock), any());
    }

    private static class RetryingHandler implements ConsumerHandler<Message> {
        @Override
        public void handleMessage(Message message, MessageAcknowledger messageAcknowledger) {
            messageAcknowledger.retry();
        }
    }
}