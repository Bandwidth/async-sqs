package com.bandwidth.sqs.consumer.strategy.loadbalance;

import static com.bandwidth.sqs.consumer.Consumer.NUM_MESSAGES_PER_REQUEST;

import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy;

public class DefaultLoadBalanceStrategy implements LoadBalanceStrategy {
    private static final int INCREASE_CONCURRENCY_LIMIT = NUM_MESSAGES_PER_REQUEST;
    private static final int DECREASE_CONCURRENCY_LIMIT = 1;
    private static final double INCREASE_CONCURRENCY_FREQUENCY = 4;//increases every x requests

    /**
     * A counter is used as opposed to Math.random() to be more consistent, predictable, and testable
     */
    private int increaseConcurrencyCounter;

    @Override
    public Action onReceiveSuccess(int messageCount) {
        if (messageCount <= DECREASE_CONCURRENCY_LIMIT) {
            return Action.Decrease;
        } else if (messageCount >= INCREASE_CONCURRENCY_LIMIT) {
            if (increaseConcurrencyCounter++ % INCREASE_CONCURRENCY_FREQUENCY == 0) {
                return Action.Increase;
            }
        }
        return Action.NoChange;
    }
}
