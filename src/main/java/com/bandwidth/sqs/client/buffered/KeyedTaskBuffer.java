package com.bandwidth.sqs.client.buffered;


import com.google.common.annotations.VisibleForTesting;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class KeyedTaskBuffer<K, D> {
    private Timer timer = new Timer();
    private final int maxBufferSize;
    private final Duration maxWait;
    private final Map<K, Buffer<D>> buffers = new HashMap<>();
    private final Task<K, D> task;

    public KeyedTaskBuffer(int maxBufferSize, Duration maxWait, Task<K, D> task) {
        this.maxBufferSize = maxBufferSize;
        this.maxWait = maxWait;
        this.task = task;
    }

    public synchronized void addData(K key, D data) {
        Buffer<D> buffer = buffers.get(key);
        if (buffer == null) {
            TimerTask timerTask = new TimerExpiredTask(key);
            buffer = new Buffer<>();
            buffers.put(key, buffer);
            timer.schedule(timerTask, maxWait.toMillis());
        }
        buffer.add(data);
        processBatchIfNeeded(key, false);
    }

    private synchronized void processBatchIfNeeded(K key, boolean expired) {
        if (!buffers.containsKey(key)) {
            return;
        }
        Buffer<D> buffer = buffers.get(key);
        List<D> batch = buffer.getContent();
        boolean isMaxSize = batch.size() >= maxBufferSize;
        if (isMaxSize || expired) {
            buffers.remove(key);
            task.run(key, batch);
        }
    }

    public interface Task<K, T> {
        void run(K key, List<T> requests);
    }

    /**
     * Internal task that runs when data in a buffers has exceeded 'maxWaitMillis'
     */
    private class TimerExpiredTask extends TimerTask {
        private final K key;

        public TimerExpiredTask(K key) {
            this.key = key;
        }

        @Override
        public void run() {
            processBatchIfNeeded(key, true);
        }
    }

    /**
     * This holds the buffered data and associated task to process that data for a specific key (queue url)
     */
    private static class Buffer<D> {
        private final List<D> batch = new ArrayList<>();

        public List<D> getContent() {
            return batch;
        }

        public void add(D data) {
            batch.add(data);
        }
    }

    @VisibleForTesting
    void setTimer(Timer timer) {
        this.timer = timer;
    }
}
