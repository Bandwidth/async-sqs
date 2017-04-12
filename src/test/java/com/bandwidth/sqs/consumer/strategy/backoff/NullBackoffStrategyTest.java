package com.bandwidth.sqs.consumer.strategy.backoff;

import static org.assertj.core.api.Assertions.assertThat;

import com.bandwidth.sqs.consumer.strategy.backoff.NullBackoffStrategy;

import org.junit.Test;

import java.time.Duration;

public class NullBackoffStrategyTest {
    private final NullBackoffStrategy backoffStrategy = new NullBackoffStrategy();

    @Test
    public void testReturnValues() {
        assertThat(backoffStrategy.getWindowSize()).isEqualByComparingTo(Duration.ZERO);
        assertThat(backoffStrategy.getDelayTime(0)).isEqualByComparingTo(Duration.ZERO);
    }

}
