package com.bandwidth.sqs.queue.buffer.task_buffer;

import java.util.List;

public interface Task<K, T> {
    void run(K key, List<T> requests);
}