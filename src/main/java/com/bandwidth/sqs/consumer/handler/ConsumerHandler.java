package com.bandwidth.sqs.consumer.handler;

import com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger;

import io.reactivex.Observable;

public interface ConsumerHandler<T> {
    /**
     * Process a message from an SQS queue.
     * You *MUST* call the messageAcknowledger once processing the message has finished.
     */
    void handleMessage(T message, MessageAcknowledger<T> messageAcknowledger);

    /**
     * This can be used to listen to events where the handler requests the number of permits to be changed
     */
    default Observable<Integer> getPermitChangeRequests() {
        return Observable.never();
    }
}