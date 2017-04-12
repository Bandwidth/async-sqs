package com.bandwidth.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.data.Percentage;
import org.junit.Test;

import java.time.Duration;

public class TimeWindowAverageTest {
    private static final int MIN_COUNT_0 = 0;
    private static final int MIN_COUNT_1 = 1;

    @Test
    public void testTimeWindow() {
        TimeWindowAverage avg = new TimeWindowAverage(Duration.ofDays(999), MIN_COUNT_0);
        avg.addData(1);
        avg.addData(10);
        avg.addData(0);
        avg.addData(0);
        assertThat(avg.getAverage()).isCloseTo(11.0 / 4.0, Percentage.withPercentage(0.00001));
    }

    @Test
    public void testRemoveValues() {
        TimeWindowAverage avg = new TimeWindowAverage(Duration.ZERO, MIN_COUNT_0);
        avg.addData(0);
        avg.addData(10);
        assertThat(avg.getAverage()).isNaN();
    }

    @Test
    public void testMinCount() {
        TimeWindowAverage avg = new TimeWindowAverage(Duration.ZERO, MIN_COUNT_1);
        avg.addData(0);
        avg.addData(10);
        assertThat(avg.getAverage()).isEqualTo(10.0);
    }

}
