package com.bandwidth.sqs.queue.buffer.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * A task to process a batch of data
 *
 * @param <K> Key: This is the same key that was given when data was inserted into the task buffer
 * @param <D> Data: The data to process
 */
public interface Task<K, D> {
    /**
     * Process a batch of data, with the given key
     *
     * @param key   This is the same key that was given when data was inserted into the task buffer
     * @param batch A set of data to process. Each value is assigned a unique String ID
     */
    void run(K key, Map<String, D> batch);

    default void run(K key, List<D> batch) {
        Map<String, D> entryMap = new HashMap<>();
        IntStream.range(0, batch.size()).forEach((id) -> {
            entryMap.put(Integer.toString(id), batch.get(id));
        });
        run(key, entryMap);
    }
}