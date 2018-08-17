package com.bandwidth.sqs.consumer.acknowledger;

import com.bandwidth.sqs.queue.SqsQueue;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.subjects.CompletableSubject;
import io.reactivex.subjects.SingleSubject;


public class MessageAcknowledger<T> {

    private final SqsQueue<T> sqsQueue;
    private final String receiptId;
    private final SingleSubject<AckMode> ackModeSingle;
    private final CompletableSubject ackingComplete;
    private final Instant expirationTime;

    public MessageAcknowledger(SqsQueue<T> sqsQueue, String receiptId, Instant expirationTime) {
        this.expirationTime = expirationTime;
        this.sqsQueue = sqsQueue;
        this.receiptId = receiptId;
        this.ackModeSingle = SingleSubject.create();
        this.ackingComplete = CompletableSubject.create();

        Duration duration = Duration.between(Instant.now(), expirationTime);
        Completable.timer(duration.toMillis(), TimeUnit.MILLISECONDS).subscribe(this::ignore);
    }

    /**
     * Delegating constructor. Any ack method called on this instance will also call it on the delegate.
     */
    public MessageAcknowledger(MessageAcknowledger<?> delegate, SqsQueue<T> sqsQueue) {
        this.sqsQueue = sqsQueue;
        this.receiptId = delegate.receiptId;
        this.ackModeSingle = delegate.ackModeSingle;
        this.ackingComplete = delegate.ackingComplete;
        this.expirationTime = delegate.expirationTime;
    }

    /**
     * Processing the message was successful, or shouldn't be tried again. The message will be deleted on SQS.
     */
    public void delete() {
        ackModeSingle.onSuccess(AckMode.DELETE);
        sqsQueue.deleteMessage(receiptId).subscribeWith(ackingComplete);
    }

    /**
     * The message will stay on SQS and will be retried when the visibility timeout expires.
     * Under high load, this shouldn't be used since there is a cap on the max number of in-flight messages
     * per SQS queue. Note that next time this message is processed, the SQS receive count will increase,
     * potentially causing it to be sent to a dead-letter queue if configured.
     * This is the fastest way to "nack" a message, since there is no request sent to SQS.
     */
    public void ignore() {
        ackModeSingle.onSuccess(AckMode.IGNORE);
        ackingComplete.onComplete();
    }

    /**
     * The message is unable to be immediately processed. The message will be put back on the front of the internal
     * buffer, and retried as soon as possible. This must be immediately followed by a decrease in number of permits,
     * or an increase in the ability to process messages to prevent this in the future. There is no request sent to SQS,
     * this is an internal operation.
     */
    public void retry() {
        ackModeSingle.onSuccess(AckMode.RETRY);
        ackingComplete.onComplete();
    }

    /**
     * The message will stay in-flight, but with the visibility timeout set to the given duration. The message will be
     * retried after the given duration.
     * Note that there is a maximum of 120,000 messages in-flight per SQS queue, and a single message cannot be extended
     * beyond a total of 12 hours. Because of these limitations, this is best used for short durations.
     * If you need to delay a high volume of messages, use replace instead.
     */
    public void delay(Duration newVisibilityTimeout) {
        ackModeSingle.onSuccess(AckMode.DELAY);
        sqsQueue.changeMessageVisibility(receiptId, newVisibilityTimeout).subscribeWith(ackingComplete);
    }

    /**
     * Processing the message failed, and you want to modify the message or add a delay before processing again.
     * The current message is deleted from SQS, and a new modified message is published in it's place.
     * The new message is published with the given delay, up to 15 minutes.
     * This is implemented using an at-least-once strategy. If this fails it may cause a duplicate message, but
     * messages will never be lost.
     * It may be a good idea to modify the message and include your own retry count, since the SQS receiveCount will be
     * reset to 0 with a new message.
     */
    public void replace(T newMessage, Optional<Duration> delay) {
        ackModeSingle.onSuccess(AckMode.MODIFY);
        doTransfer(newMessage, sqsQueue, delay).subscribeWith(ackingComplete);
    }

    public void replace(T newMessage) {
        replace(newMessage, Optional.empty());
    }

    /**
     * Same as replace, but the modified message is published to a different SQS queue.
     * If you want to publish a modified or delayed message to the SAME queue, you must use replace or requeue instead.
     */
    public void transfer(T newMessage, SqsQueue<T> newQueue, Optional<Duration> delay) {
        ackModeSingle.onSuccess(AckMode.TRANSFER);
        doTransfer(newMessage, newQueue, delay).subscribeWith(ackingComplete);
    }

    public void transfer(T newMessage, SqsQueue<T> newQueue){
        transfer(newMessage, newQueue, Optional.empty());
    }

    private Completable doTransfer(T newMessage, SqsQueue<T> newQueue, Optional<Duration> delay) {
        return newQueue.publishMessage(newMessage, delay)
                .flatMapCompletable((msgId) -> {
                    Completable completable = sqsQueue.deleteMessage(receiptId);
                    return completable;
                });
    }

    /**
     * @return a Single that is completed when the ack mode is chosen, or the visibility timeout expires. If it expires,
     * this will return AckMode.IGNORE
     */
    public Single<AckMode> getAckMode() {
        return ackModeSingle;
    }

    /**
     * @return a Completable that completes when the acking action has completed. For example, if the ack mode is
     * DELETE, this will be completed when the message was deleted from SQS
     */
    public Completable getCompletable() {
        return ackingComplete;
    }

    public SqsQueue<T> getQueue() {
        return sqsQueue;
    }

    public enum AckMode {
        DELETE,
        IGNORE,
        RETRY,
        DELAY,
        MODIFY,
        TRANSFER;

        /**
         * A "successful" ack mode means the (potentially modified) message will never be consumed again by the same
         * consumer.
         *
         * @return is this AckMode is considered 'successful'
         */
        public boolean isSuccessful() {
            return this == AckMode.DELETE || this == AckMode.TRANSFER;
        }
    }
}