package com.bandwidth.sqs.consumer;

import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy.Action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

/**
 * Manages all interactions between consumers. This includes assigning the number of allocated
 * requests, and managing threads the process messages retrieved by the consumers.
 */

public class ConsumerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerManager.class);

    private final Map<Consumer, Integer> allocatedInFlightRequests = new HashMap<>();
    private Timer timer = new Timer();

    /**
     * Contains a set of Consumers ordered by the number of allocated in flight requests.
     * Whenever all of the globally allocated in-flight requests are allocated,
     * consumers are eligible to steal requests from the consumer with most requests,
     * to evenly consume from queues.
     */
    private final PriorityQueue<Consumer> consumersOrderedByRequests =
            new PriorityQueue<>((o1, o2) ->
                    getAllocatedInFlightRequestsCount(o2) - getAllocatedInFlightRequestsCount(o1));

    private int currentGlobalAllocatedRequests;
    private final int maxGlobalAllocatedRequests;
    private final ExecutorService threadPool;

    /**
     * @param maxLoadBalancedRequests This should be the number of concurrent in-flight requests needed to fully
     *                                saturate the network. Each consumer will always have at least 1 in-flight request,
     *                                and additional requests up to this value will be load balanced across non-empty
     *                                SQS queues. You will probably need to do performance testing to find a good value.
     *                                If this is too low, you may notice low throughput for some queues. If this value
     *                                is too high, you may notice connection timeouts or high memory usage. For a good
     *                                starting value, 64 can saturate a 100 mbps network with small message payloads and
     *                                a ~40ms round-trip latency.
     * @param threadPool              An executor used to process consumer handlers
     */
    public ConsumerManager(int maxLoadBalancedRequests, ExecutorService threadPool) {
        this.threadPool = threadPool;
        this.maxGlobalAllocatedRequests = maxLoadBalancedRequests;
    }

    public synchronized void addConsumer(Consumer consumer) {
        consumersOrderedByRequests.add(consumer);
    }

    public synchronized void removeConsumer(Consumer consumer) {
        int allocatedRequests = getAllocatedInFlightRequestsCount(consumer);
        setAllocatedInFlightRequests(consumer, 0);
        currentGlobalAllocatedRequests -= allocatedRequests;
        consumersOrderedByRequests.remove(consumer);
    }

    public synchronized int getCurrentGlobalAllocatedRequests() {
        return currentGlobalAllocatedRequests;
    }

    public synchronized int getAllocatedInFlightRequestsCount(Consumer consumer) {
        return Optional.ofNullable(allocatedInFlightRequests.get(consumer)).orElse(0);
    }

    public synchronized void updateAllocatedInFlightRequests(Consumer consumer, Update update) {
        Action action = update.getAction(getAllocatedInFlightRequestsCount(consumer));
        if (action == Action.Decrease) {
            decreaseAllocatedInFlightRequests(consumer);
        } else if (action == Action.Increase) {
            increaseAllocatedInFlightRequests(consumer);
        }
    }

    public void queueTask(Runnable task) {
        threadPool.execute(task);
    }

    public void scheduleTask(TimerTask task, Duration delay) {
        timer.schedule(task, delay.toMillis());
    }

    private synchronized void setAllocatedInFlightRequests(Consumer consumer, int numRequests) {
        consumersOrderedByRequests.remove(consumer);
        allocatedInFlightRequests.put(consumer, numRequests);
        consumersOrderedByRequests.add(consumer);
    }

    private synchronized void decreaseAllocatedInFlightRequests(Consumer consumer) {
        int previousValue = getAllocatedInFlightRequestsCount(consumer);
        if (previousValue > 0) {
            currentGlobalAllocatedRequests -= 1;
            setAllocatedInFlightRequests(consumer, previousValue - 1);
        }
    }

    private synchronized void increaseAllocatedInFlightRequests(Consumer consumer) {
        if (currentGlobalAllocatedRequests < maxGlobalAllocatedRequests) {
            setAllocatedInFlightRequests(consumer, getAllocatedInFlightRequestsCount(consumer) + 1);
            currentGlobalAllocatedRequests += 1;
        } else { //there are no free requests left, try to balance consumers by taking from the "largest" consumer
            Consumer largestConsumer = consumersOrderedByRequests.peek();
            int smallerConsumerAllocatedRequests = getAllocatedInFlightRequestsCount(consumer);
            int largestConsumerAllocatedRequests = getAllocatedInFlightRequestsCount(largestConsumer);
            if (largestConsumerAllocatedRequests - smallerConsumerAllocatedRequests >= 2) {
                setAllocatedInFlightRequests(largestConsumer, largestConsumerAllocatedRequests - 1);
                setAllocatedInFlightRequests(consumer, smallerConsumerAllocatedRequests + 1);
            }
        }
    }

    void setTimer(Timer timer) {
        this.timer = timer;
    }
}