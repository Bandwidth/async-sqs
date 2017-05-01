package com.bandwidth.sqs.queue.buffer.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public interface Task<K, T> {
    void run(K key, Map<String, T> requests);

    default void run(K key, List<T> requests) {
        Map<String, T> entryMap = new HashMap<>();
        IntStream.range(0, requests.size()).forEach((id) -> {
            entryMap.put(Integer.toString(id), requests.get(id));
        });
        run(key, entryMap);
    }
}