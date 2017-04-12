package com.bandwidth.sqs.consumer.strategy.expiration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class NeverExpiresStrategyTest {

    private final NeverExpiresStrategy neverExpiresStrategy = new NeverExpiresStrategy();

    @Test
    public void testThatFalseEqualsFalse() {
        assertThat(neverExpiresStrategy.isExpired(null)).isFalse();
    }
}
