package com.bandwidth.sqs.consumer;

import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy;

interface Update {
    LoadBalanceStrategy.Action getAction(int oldValue);
}