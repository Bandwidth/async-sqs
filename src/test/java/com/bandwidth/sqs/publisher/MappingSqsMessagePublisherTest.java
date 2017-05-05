package com.bandwidth.sqs.publisher;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import java.util.Optional;

import io.reactivex.Single;

public class MappingSqsMessagePublisherTest {

    private static final int ORIGINAL_VALUE = 41;
    private static final int MAPPED_VALUE = 42;
    private static final String MESSAGE_ID = "message-id";

    private final SqsMessagePublisher<Integer> delegateMock = mock(SqsMessagePublisher.class);

    private final MappingSqsMessagePublisher<Integer, Integer> publisher =
            new MappingSqsMessagePublisher<>(delegateMock, (value) -> value + 1);

    public MappingSqsMessagePublisherTest() {
        when(delegateMock.publishMessage(anyInt(), any())).thenReturn(Single.just(MESSAGE_ID));
    }

    @Test
    public void testPublishMessage() {
        Single<String> single = publisher.publishMessage(ORIGINAL_VALUE);
        verify(delegateMock).publishMessage(MAPPED_VALUE, Optional.empty());
        single.test().assertValue(MESSAGE_ID);
    }
}
