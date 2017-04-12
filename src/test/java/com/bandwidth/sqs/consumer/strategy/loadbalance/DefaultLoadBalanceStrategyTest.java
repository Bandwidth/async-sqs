package com.bandwidth.sqs.consumer.strategy.loadbalance;

import static org.assertj.core.api.Assertions.assertThat;

import com.bandwidth.sqs.consumer.Consumer;
import com.bandwidth.sqs.consumer.strategy.loadbalance.DefaultLoadBalanceStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy.Action;

import org.junit.Test;

public class DefaultLoadBalanceStrategyTest {
    private static final int NO_CHANGE_NUM_MESSAGES = 5;
    private final LoadBalanceStrategy strategy = new DefaultLoadBalanceStrategy();

    @Test
    public void emptyResponseTest() {
        Action action = strategy.onReceiveSuccess(0);
        assertThat(action).isEqualTo(Action.Decrease);
    }

    @Test
    public void fullResponseTest() {
        assertThat(strategy.onReceiveSuccess(Consumer.NUM_MESSAGES_PER_REQUEST)).isEqualTo(Action.Increase);
        assertThat(strategy.onReceiveSuccess(Consumer.NUM_MESSAGES_PER_REQUEST)).isEqualTo(Action.NoChange);
        assertThat(strategy.onReceiveSuccess(Consumer.NUM_MESSAGES_PER_REQUEST)).isEqualTo(Action.NoChange);
        assertThat(strategy.onReceiveSuccess(Consumer.NUM_MESSAGES_PER_REQUEST)).isEqualTo(Action.NoChange);
        assertThat(strategy.onReceiveSuccess(Consumer.NUM_MESSAGES_PER_REQUEST)).isEqualTo(Action.Increase);
    }

    @Test
    public void noChangeTest() {
        assertThat(strategy.onReceiveSuccess(NO_CHANGE_NUM_MESSAGES)).isEqualTo(Action.NoChange);
    }

}
