package com.bandwidth.sqs.consumer.strategy.loadbalance;

public interface LoadBalanceStrategy {

    Action onReceiveSuccess(int messageCount);

    enum Action {
        Increase,
        NoChange,
        Decrease
    }
}
