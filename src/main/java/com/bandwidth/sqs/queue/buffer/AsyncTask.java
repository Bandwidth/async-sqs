package com.bandwidth.sqs.queue.buffer;

import java.util.List;

import io.reactivex.Single;

public interface AsyncTask<K, T, R> {
    Single<R> run(K key, List<T> requests);
}