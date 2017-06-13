package com.bandwidth.sqs.consumer;

import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy.Action;
import com.bandwidth.sqs.queue.SqsMessage;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;

import io.reactivex.Completable;
import io.reactivex.functions.Function;

/**
 * Manages all interactions between consumers. This includes assigning the number of allocated
 * requests, and managing threads the process messages retrieved by the consumers.
 */

public class SqsConsumerManager<T> {
    private static final Comparator<PriorityTask> PRIORITY_RUNNABLE_COMPARATOR =
            Comparator.comparingInt(PriorityTask::getPriority);

    private final Map<SqsConsumer, Integer> allocatedInFlightRequests = new HashMap<>();
    private Timer timer = new Timer();

    /**
     * Contains a set of Consumers ordered by the number of allocated in flight requests.
     * Whenever all of the globally allocated in-flight requests are allocated,
     * consumers are eligible to steal requests from the consumer with most requests,
     * to evenly consume from queues.
     */
    private final PriorityQueue<SqsConsumer> consumersOrderedByRequests =
            new PriorityQueue<>((o1, o2) ->
                    getAllocatedInFlightRequestsCount(o2) - getAllocatedInFlightRequestsCount(o1));

    private int currentGlobalAllocatedRequests;
    private final int maxGlobalAllocatedRequests;
    private final ExecutorService threadPool;
    private final PriorityBlockingQueue<PriorityTask<T>> threadPoolQueue =
            new PriorityBlockingQueue<>(1, PRIORITY_RUNNABLE_COMPARATOR);
    private final Semaphore taskPermits;//total number of pending tasks that can run

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
     * @param numHandlerPermits       This is the maximum number of message handlers than can run concurrently. This
     *                                is important when there are multiple consumers running with different
     *                                priorities set. If this number is too high, lower priority consumers will
     *                                exhaust system resources and slow down higher priority consumers. If this is
     *                                set too low this will lower performance for all consumers using this manager.
     *                                Note that this is different than the max number of threads configured in the
     *                                thread pool. The number of threads will only restrict synchronous processing,
     *                                while these permits will restrict both sync and async processing. The max
     *                                number of threads should be set to a high value as to not be the bottle neck,
     *                                and these permits should be used to limit concurrent processing. Performance
     *                                tests should be used to find an optimal value for this. A good starting value
     *                                might be around 500.
     */
    public SqsConsumerManager(int maxLoadBalancedRequests, ExecutorService threadPool, int numHandlerPermits) {
        this.threadPool = threadPool;
        this.maxGlobalAllocatedRequests = maxLoadBalancedRequests;
        this.taskPermits = new Semaphore(numHandlerPermits);
    }

    public synchronized void addConsumer(SqsConsumer consumer) {
        consumersOrderedByRequests.add(consumer);
    }

    public synchronized void removeConsumer(SqsConsumer consumer) {
        int allocatedRequests = getAllocatedInFlightRequestsCount(consumer);
        setAllocatedInFlightRequests(consumer, 0);
        currentGlobalAllocatedRequests -= allocatedRequests;
        consumersOrderedByRequests.remove(consumer);
    }

    public synchronized int getCurrentGlobalAllocatedRequests() {
        return currentGlobalAllocatedRequests;
    }

    public synchronized int getAllocatedInFlightRequestsCount(SqsConsumer consumer) {
        return Optional.ofNullable(allocatedInFlightRequests.get(consumer)).orElse(0);
    }

    public synchronized void updateAllocatedInFlightRequests(SqsConsumer consumer, Update update) {
        Action action = update.getAction(getAllocatedInFlightRequestsCount(consumer));
        if (action == Action.Decrease) {
            decreaseAllocatedInFlightRequests(consumer);
        } else if (action == Action.Increase) {
            increaseAllocatedInFlightRequests(consumer);
        }
    }

    /**
     *
     * @param task A function that takes an sqs message to process, that returns a Completable when the task is
     *             considered to be completed
     * @param priority The priority of the task. Higher priority (lower value) is run first.
     * @param consumer The consumer that owns this task.
     */
    public void queueTask(Function<SqsMessage<T>, Completable> task, int priority, SqsConsumer<T> consumer) {

        //queue a task that we want to run on the thread pool. If there are more tasks than threads
        //the highest priority tasks will run first
        threadPoolQueue.add(new PriorityTask<>(priority, consumer, (msg) -> {
            Completable.defer(() -> task.apply(msg))
                    .doFinally(taskPermits::release).subscribe();
        }));

        threadPool.execute(() -> {
            taskPermits.acquireUninterruptibly();//synchronously block if a permit is not available
            PriorityTask<T> runnable;
            SqsMessage<T> message;
            synchronized (threadPoolQueue) {
                runnable = threadPoolQueue.remove();
                message = runnable.getNextMessage();
                //let the consumer add additional tasks to the queue before the next thread can pick up a task
                runnable.updateConsumer();
            }
            runnable.accept(message);//run the highest priority task

        });
    }

    public void scheduleTask(TimerTask task, Duration delay) {
        timer.schedule(task, delay.toMillis());
    }

    private synchronized void setAllocatedInFlightRequests(SqsConsumer consumer, int numRequests) {
        consumersOrderedByRequests.remove(consumer);
        allocatedInFlightRequests.put(consumer, numRequests);
        consumersOrderedByRequests.add(consumer);
    }

    private synchronized void decreaseAllocatedInFlightRequests(SqsConsumer consumer) {
        int previousValue = getAllocatedInFlightRequestsCount(consumer);
        if (previousValue > 0) {
            currentGlobalAllocatedRequests -= 1;
            setAllocatedInFlightRequests(consumer, previousValue - 1);
        }
    }

    private synchronized void increaseAllocatedInFlightRequests(SqsConsumer consumer) {
        if (currentGlobalAllocatedRequests < maxGlobalAllocatedRequests) {
            setAllocatedInFlightRequests(consumer, getAllocatedInFlightRequestsCount(consumer) + 1);
            currentGlobalAllocatedRequests += 1;
        } else { //there are no free requests left, try to balance consumers by taking from the "largest" consumer
            SqsConsumer largestConsumer = consumersOrderedByRequests.peek();
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