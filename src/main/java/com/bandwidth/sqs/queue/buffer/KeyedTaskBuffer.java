package com.bandwidth.sqs.queue.buffer;

import com.bandwidth.sqs.queue.buffer.task.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

/**
 * A buffer that buffers individual data, collecting it in bucket by key, then running a task to batch process the data
 *
 * @param <K> Key
 * @param <D> Data
 */
public class KeyedTaskBuffer<K, D> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyedTaskBuffer.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final int maxBufferSize;
    private final Duration maxWait;
    private final Map<K, List<D>> buffers = new HashMap<>();
    private final Task<K, D> task;

    /**
     * Construct a KeyedTaskBuffer with a single threaded ScheduledThreadPool to run tasks when a
     * batch is timed out due to maxWait
     */
    public KeyedTaskBuffer(int maxBufferSize, Duration maxWait, Task<K, D> task) {
        this(Executors.newScheduledThreadPool(1), maxBufferSize, maxWait, task);
    }

    /**
     * Construct a KeyedTaskBuffer with all parameters
     * @param scheduledExecutorService - The executor used for task execution when a batch's wait time has expired
     * @param maxBufferSize - The maximum amount of task data to hold before flushing the batch and executing it
     * @param maxWait - The maximum amount of time to hold a batch before flushing it regardless of size
     * @param task - The task that executes the batched data
     */
    public KeyedTaskBuffer(ScheduledExecutorService scheduledExecutorService, int maxBufferSize,
                           Duration maxWait, Task<K, D> task) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.maxBufferSize = maxBufferSize;
        this.maxWait = maxWait;
        this.task = task;
    }


    /**
     * Add task data to a keyed buffer.  If the buffer is full the batch will be executed immediately
     * on the calling thread. Otherwise the task will be executed by the scheduleExecutorService when
     * the maxWait timeout has elapsed.
     * @param key - Task Key.  Tasks with common keys are batched
     * @param data - Task Data
     */
    public void addData(final K key, D data) {
        List<D> readyBatch;

        synchronized (this) {
            List<D> buffer = buffers.get(key);
            if (buffer == null) {
                buffer = new ArrayList<>();
                buffers.put(key, buffer);

                scheduledExecutorService.schedule(() -> {
                    try {
                        processExpiredBatch(key);
                    } catch (Exception e) {
                        LOG.error("Exception running task with key {}", key,  e);
                    }
                }, maxWait.toMillis(), TimeUnit.MILLISECONDS);
            }
            buffer.add(data);

            //check if the batch is ready while holding the lock to prevent batches from
            //going above the maximum size
            readyBatch = fetchAndRemoveBatchIfReady(key, false);
        }

        //Execute the ready batch without holding the lock.
        if (readyBatch != null) {
            task.run(key, readyBatch);
        }
    }

    /**
     * Process a batch of tasks when the timeout has occurred. This is executed by the
     * scheduledExecutorService
     * @param key The task key
     */
    private void processExpiredBatch(final K key) {
        List<D> batch = fetchAndRemoveBatchIfReady(key, true);
        if (batch != null) {
            task.run(key, batch);
        }
    }

    /**
     * Fetch and Remove a batch from the buffer map if the buffer is full or the max wait has expired
     * A batch returned from this function can be safely processed without holding the lock
     * @param key The task key
     * @param expired this is true when invoked from the scheduler thread and false when invoked
     *                after adding a new value to the batch
     * @return
     */
    private synchronized List<D> fetchAndRemoveBatchIfReady(K key, boolean expired) {
        if (!buffers.containsKey(key)) {
            return null;
        }

        List<D> buffer = buffers.get(key);
        boolean isMaxSize = buffer.size() >= maxBufferSize;
        if (isMaxSize || expired) {
            buffers.remove(key);
            return buffer;
        }
        return null;
    }


    @PreDestroy
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
