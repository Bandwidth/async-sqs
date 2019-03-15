package com.bandwidth.sqs.queue.buffer;

import com.bandwidth.sqs.queue.buffer.task.Task;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A buffer that buffers individual data, collecting it in bucket by key, then running a task to batch process the data
 *
 * @param <K> Key
 * @param <D> Data
 */
public class KeyedTaskBuffer<K, D> {
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService taskExecutorService;
    private final int maxBufferSize;
    private final Duration maxWait;
    private final Map<K, List<D>> buffers = new HashMap<>();
    private final Task<K, D> task;

    /**
     * Construct a KeyedTaskBuffer with a default execution policy:
     *     A single threaded ScheduledThreadPool for buffer timeout invocation
     *     A single threaded ExecutorService for that tasks will be submitted to
     */
    public KeyedTaskBuffer(int maxBufferSize, Duration maxWait, Task<K, D> task) {
        this(Executors.newScheduledThreadPool(1), Executors.newSingleThreadExecutor(),
                maxBufferSize, maxWait, task);
    }

    /**
     * Construct a KeyedTaskBuffer with all parameters
     * @param scheduledExecutorService - The executor used for buffer timeout invocation
     * @param taskExecutorService - The executor used for invoking tasks when the buffer is flushed
     * @param maxBufferSize - The maximum amount of task data to hold before flushing the batch and executing it
     * @param maxWait - The maximum amount of time to hold a batch before flushing it regardless of size
     * @param task - The task that executes the batched data
     */
    public KeyedTaskBuffer(ScheduledExecutorService scheduledExecutorService, ExecutorService taskExecutorService,
                           int maxBufferSize, Duration maxWait, Task<K, D> task) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.taskExecutorService = taskExecutorService;
        this.maxBufferSize = maxBufferSize;
        this.maxWait = maxWait;
        this.task = task;
    }


    /**
     * Add task data to a keyed buffer.  If the buffer is full the batch will be submitted immediately
     * to the taskExecutorService.  Otherwise the batch will be submitted when the maxWait timeout occurs
     * @param key - Task Key.  Tasks with common keys are batched
     * @param data - Task Data
     */
    public synchronized void addData(final K key, D data) {
        List<D> buffer = buffers.get(key);
        if (buffer == null) {
            buffer = new ArrayList<>();
            buffers.put(key, buffer);

            scheduledExecutorService.schedule(() ->{
                try {
                    processBatchIfNeeded(key, true);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }, maxWait.toMillis(), TimeUnit.MILLISECONDS);
        }
        buffer.add(data);
        processBatchIfNeeded(key, false);
    }

    /**
     * Process a batch of tasks if the batch is full OR if the timeout has occurred
     * @param key The task key
     * @param expired this is true when invoked from the scheduler thread and false when invoked
     *                after adding a new value to the batch
     */
    private synchronized void processBatchIfNeeded(final K key, boolean expired) {
        if (!buffers.containsKey(key)) {
            return;
        }
        final List<D> buffer = buffers.get(key);
        boolean isMaxSize = buffer.size() >= maxBufferSize;
        if (isMaxSize || expired) {
            buffers.remove(key);
            //The task is ready, submit for execution on a thread that is not holding the lock
            taskExecutorService.submit(() -> task.run(key, buffer));
        }
    }
}
