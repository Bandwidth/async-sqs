package com.bandwidth.sqs.queue;


import org.immutables.value.Value.Immutable;

import java.text.MessageFormat;

@Immutable
public interface RedrivePolicy {
    String REDRIVE_POLICY_PATTERN = "'{'\"maxReceiveCount\":\"{0}\",\"deadLetterTargetArn\":\"{1}\"'}'";

    int getMaxReceiveCount();

    String getDeadLetterTargetArn();

    default String toAttributeString() {
        return MessageFormat.format(REDRIVE_POLICY_PATTERN, getMaxReceiveCount(), getDeadLetterTargetArn());
    }

    static ImmutableRedrivePolicy.Builder builder() {
        return ImmutableRedrivePolicy.builder();
    }
}
