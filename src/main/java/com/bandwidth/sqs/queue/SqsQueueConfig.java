package com.bandwidth.sqs.queue;

import com.amazonaws.regions.Regions;

import org.immutables.value.Value.Immutable;

@Immutable
public abstract class SqsQueueConfig {

    /**
     * SQS queue name, must be unique within an AWS account
     */
    public abstract String getName();

    /**
     * Region where this SQS queue exists
     */
    public abstract Regions getRegion();

    /**
     * The queue attributes
     */
    public abstract SqsQueueAttributes getAttributes();

    public static ImmutableSqsQueueConfig.Builder builder(){
        return ImmutableSqsQueueConfig.builder();
    }
}