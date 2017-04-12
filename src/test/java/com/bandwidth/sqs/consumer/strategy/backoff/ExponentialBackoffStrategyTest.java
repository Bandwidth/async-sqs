package com.bandwidth.sqs.consumer.strategy.backoff;

import static org.assertj.core.api.Assertions.assertThat;

import com.bandwidth.sqs.consumer.strategy.backoff.ExponentialBackoffStrategy;

import org.junit.Test;

import java.time.Duration;

public class ExponentialBackoffStrategyTest {
    private static final Duration WINDOW_SIZE = Duration.ofMillis(1000);
    private static final Duration MAX_DELAY = Duration.ofMillis(1000);
    private static final int EXPONENT_1 = 1;
    private static final int EXPONENT_2 = 2;
    private static final double ZERO_FAILURE = 0.0;
    private static final double HALF_FAILURE = 0.5;
    private static final double ALL_FAILURE = 1.0;

    @Test
    public void testGetWindowSizeMillis() {
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(WINDOW_SIZE, MAX_DELAY,
                EXPONENT_1);
        assertThat(strategy.getWindowSize()).isEqualTo(WINDOW_SIZE);
    }

    @Test
    public void testLinear() {
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(WINDOW_SIZE, MAX_DELAY,
                EXPONENT_1);
        assertThat(strategy.getDelayTime(ZERO_FAILURE).toMillis()).isEqualTo(0);
        assertThat(strategy.getDelayTime(HALF_FAILURE).toMillis()).isEqualTo(500);
        assertThat(strategy.getDelayTime(ALL_FAILURE).toMillis()).isEqualTo(1000);
    }

    @Test
    public void testExponential() {
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(WINDOW_SIZE, MAX_DELAY,
                EXPONENT_2);
        assertThat(strategy.getDelayTime(ZERO_FAILURE).toMillis()).isEqualTo(0);
        assertThat(strategy.getDelayTime(HALF_FAILURE).toMillis()).isEqualTo(250);
        assertThat(strategy.getDelayTime(ALL_FAILURE).toMillis()).isEqualTo(1000);
    }
}
