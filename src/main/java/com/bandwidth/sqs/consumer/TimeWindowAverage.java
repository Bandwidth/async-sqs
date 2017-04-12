package com.bandwidth.sqs.consumer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

public class TimeWindowAverage {
    private final Deque<Data> queue = new ArrayDeque<>();
    private final Duration windowSize;
    private long sum = 0;
    private final int minCount;
    private final Clock clock = Clock.systemUTC();

    public TimeWindowAverage(Duration windowSize, int minCount) {
        this.windowSize = windowSize;
        this.minCount = minCount;
    }

    public synchronized double getAverage() {
        removeOldValues();
        return ((double) sum) / queue.size();
    }

    public synchronized void addData(long value) {
        queue.add(new Data(value, clock.instant()));
        sum += value;
        removeOldValues();
    }

    private synchronized void removeOldValues() {
        Instant expirationTime = Instant.now().minus(windowSize);
        while (queue.size() > minCount && !queue.peek().insertTime.isAfter(expirationTime)) {
            sum -= queue.remove().value;
        }
    }

    private static class Data {
        public final Instant insertTime;
        public final long value;

        public Data(long value, Instant insertTime) {
            this.value = value;
            this.insertTime = insertTime;
        }
    }
}
