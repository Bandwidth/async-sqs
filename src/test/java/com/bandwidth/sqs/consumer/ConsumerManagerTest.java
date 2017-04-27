package com.bandwidth.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy.Action;

import org.junit.Test;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

public class ConsumerManagerTest {

    private static final int MAX_LOAD_BALANCED_REQUESTS = 2;

    private final ExecutorService threadPoolMock = mock(ExecutorService.class);
    private final Consumer consumerMock = mock(Consumer.class);
    private final Consumer consumerMock2 = mock(Consumer.class);

    private ConsumerManager consumerManager;

    public ConsumerManagerTest() {
        consumerManager = new ConsumerManager(MAX_LOAD_BALANCED_REQUESTS, threadPoolMock);
        consumerManager.addConsumer(consumerMock);
        consumerManager.addConsumer(consumerMock2);
    }

    @Test
    public void queueReadyConsumerTest() {
        Runnable task = mock(Runnable.class);
        consumerManager.queueTask(task);
        verify(threadPoolMock).execute(task);
    }

    @Test
    public void scheduleTaskTest() throws InterruptedException {
        Timer timer = mock(Timer.class);
        consumerManager.setTimer(timer);
        TimerTask timerTask = mock(TimerTask.class);
        consumerManager.scheduleTask(timerTask, Duration.ZERO);
        verify(timer).schedule(timerTask, 0);
    }

    @Test
    public void testIncreaseAllocatedInFlightRequests() {
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Increase);
        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock)).isEqualTo(1);
    }

    @Test
    public void testDecreaseAllocatedInFlightRequests() {
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Increase);
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Decrease);
        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock)).isEqualTo(0);
    }

    @Test
    public void testNoChangeAllocatedInFlightRequests() {
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.NoChange);
        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock)).isEqualTo(0);
    }

    @Test
    public void testDecreaseBelowZeroAllocatedInFlightRequests() {
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Decrease);
        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock)).isEqualTo(0);
    }

    @Test
    public void testIncreaseUnbalancedAllocatedInFlightRequests() {
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Increase);
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Increase);
        consumerManager.updateAllocatedInFlightRequests(consumerMock2, (oldValue) -> Action.Increase);

        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock)).isEqualTo(1);
        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock2)).isEqualTo(1);
    }

    @Test
    public void testIncreaseBalancedAllocatedInFlightRequests() {
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Increase);
        consumerManager.updateAllocatedInFlightRequests(consumerMock2, (oldValue) -> Action.Increase);
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Increase);

        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock)).isEqualTo(1);
        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock2)).isEqualTo(1);
    }

    @Test
    public void testRemoveConsumer() {
        consumerManager.updateAllocatedInFlightRequests(consumerMock, (oldValue) -> Action.Increase);
        consumerManager.removeConsumer(consumerMock);

        assertThat(consumerManager.getAllocatedInFlightRequestsCount(consumerMock)).isEqualTo(0);
        assertThat(consumerManager.getCurrentGlobalAllocatedRequests()).isEqualTo(0);
    }
}