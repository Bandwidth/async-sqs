package com.bandwidth.sqs.consumer;

import static java.util.Objects.requireNonNull;

import com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger;
import com.bandwidth.sqs.consumer.strategy.expiration.ExpirationStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.DefaultLoadBalanceStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy.Action;
import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;
import com.bandwidth.sqs.consumer.handler.ConsumerHandler;
import com.bandwidth.sqs.queue.SqsMessage;
import com.bandwidth.sqs.queue.SqsQueue;
import com.bandwidth.sqs.queue.SqsQueueAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.Completable;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.CompletableSubject;

import static com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger.AckMode;

import javax.annotation.PreDestroy;

public class SqsConsumer<T> {
    public static final int NUM_MESSAGES_PER_REQUEST = 10;
    public static final Duration LOAD_BALANCED_REQUEST_WAIT_TIME = Duration.ofSeconds(1);
    public static final Duration MAX_WAIT_TIME = Duration.ofSeconds(20);
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    static final long MESSAGE_SUCCESS = 0;
    static final long MESSAGE_FAILURE = 1;

    private static final int TIME_WINDOW_MIN_COUNT = 10;

    private static final Logger LOG = LoggerFactory.getLogger(SqsConsumer.class);

    private final SqsQueue<T> sqsQueue;
    private final AtomicInteger maxPermits;
    private final AtomicInteger remainingPermits;
    private final ConsumerHandler<T> handler;

    private final int maxQueueSize;
    private final BackoffStrategy backoffStrategy;
    private final SqsConsumerManager<T> manager;
    private final ExpirationStrategy expirationStrategy;
    private final AtomicInteger inFlightLoadBalancedRequests = new AtomicInteger(0);
    private final AtomicBoolean longPollRequestInFlight = new AtomicBoolean(false);
    private final CompletableSubject shutdownCompletable = CompletableSubject.create();
    private final Disposable permitChangeDisposable;
    private final SqsQueueAttributes queueAttributes;
    private final int priority;
    private final boolean autoExpire;

    private ArrayDeque<SqsMessage<T>> messageBuffer = new ArrayDeque<>();
    private boolean waitingInQueue = false;
    private TimeWindowAverage failureAverage = null;
    private Instant backoffEndTime = Instant.EPOCH;
    private LoadBalanceStrategy loadBalanceStrategy = new DefaultLoadBalanceStrategy();
    private boolean shuttingDown = false;

    /**
     * Adds a consumer for a specific SQS Queue. Once a consumer is started, the handler will be called from a
     * thread-pool to process messages. It is safe to use blocking calls in the handler as this will not impact
     * performance if a sufficient number of `workerThreads` are configured in the SqsConsumerManager. Only one consumer
     * is normally needed per SQS Queue. A single long-polling request is always in-flight for each consumer in addition
     * to the load-balanced requests configured in the `SqsConsumerManager`.
     */
    public SqsConsumer(SqsConsumerBuilder<T> builder) {
        this.handler = requireNonNull(builder.consumerHandler);
        this.backoffStrategy = requireNonNull(builder.backoffStrategy);
        this.manager = requireNonNull(builder.consumerManager);
        this.expirationStrategy = requireNonNull(builder.expirationStrategy);
        this.sqsQueue = builder.sqsQueue;
        this.priority = builder.priority;
        this.autoExpire = builder.autoExpire;
        this.queueAttributes = sqsQueue.getAttributes().blockingGet();
        this.maxPermits = new AtomicInteger(builder.numPermits);
        this.remainingPermits = new AtomicInteger(builder.numPermits);
        this.maxQueueSize = Math.max(NUM_MESSAGES_PER_REQUEST, builder.bufferSize);

        initFailureAverage();
        manager.addConsumer(this);
        permitChangeDisposable = handler
                .getPermitChangeRequests()
                .subscribe(this::setNumPermits);
    }

    public void setLoadBalanceStrategy(LoadBalanceStrategy strategy) {
        this.loadBalanceStrategy = strategy;
    }

    public SqsQueue<T> getQueue() {
        return sqsQueue;
    }

    public int getBufferSize() {
        return maxQueueSize;
    }

    public BackoffStrategy getBackoffStrategy() {
        return backoffStrategy;
    }

    public ExpirationStrategy getExpirationStrategy() {
        return expirationStrategy;
    }

    public int getPriority() {
        return priority;
    }

    /**
     * Consumer cannot be started after it has been shutdown
     */
    public void start() {
        update();
    }

    public synchronized void update() {
        queueForProcessingIfNeeded();
        if (shuttingDown) {
            if (isShutdown()) {
                shutdownCompletable.onComplete();
            }
        } else {
            startNewRequestsIfNeeded();
        }
    }

    public void setNumPermits(int newValue) {
        int oldValue = maxPermits.getAndSet(newValue);
        remainingPermits.addAndGet(newValue - oldValue);
        update();
    }

    public int getNumPermits() {
        return maxPermits.get();
    }

    /**
     * Shuts down the consumer, and blocks until the shutdown completes with a
     * default timeout of 30 seconds
     */
    @PreDestroy
    public boolean shutdown() {
        return shutdown(DEFAULT_SHUTDOWN_TIMEOUT);
    }

    /**
     * Shuts down the consumer, and blocks until the shutdown completes.
     * Throws a timeout exception if it fails to shutdown in the timeout specified
     */
    public boolean shutdown(Duration timeout) {
        LOG.info("Beginning graceful shutdown of SQS Consumer");
        permitChangeDisposable.dispose();
        boolean completed = shutdownAsync()
                .blockingAwait(timeout.getSeconds(), TimeUnit.SECONDS);

        if (completed) {
            LOG.info("Graceful shutdown of SQS consumer complete");
        } else {
            LOG.error("Error shutting down SQS Consumer");
        }
        return completed;
    }

    /**
     * Starts shutdown, returning a Completable that completes when the shutdown completes
     */
    public Completable shutdownAsync() {
        this.shuttingDown = true;

        update();

        shutdownCompletable.doFinally(() -> {
            manager.removeConsumer(this);
        }).subscribe();

        return shutdownCompletable;
    }

    public synchronized boolean isShutdown() {
        return shuttingDown
                && !longPollRequestInFlight.get()
                && inFlightLoadBalancedRequests.get() == 0
                && messageBuffer.isEmpty()
                && remainingPermits.get() == maxPermits.get();
    }

    void setMessageBuffer(ArrayDeque<SqsMessage<T>> messageBuffer) {
        this.messageBuffer = messageBuffer;
    }


    private void initFailureAverage() {
        failureAverage = new TimeWindowAverage(backoffStrategy.getWindowSize(), TIME_WINDOW_MIN_COUNT);
        for (int i = 0; i < TIME_WINDOW_MIN_COUNT; i++) {
            //consumer's initial behavior is as if nothing has failed, so initialize the average to success
            failureAverage.addData(MESSAGE_SUCCESS);
        }
    }

    private boolean isBlockedByBackoffDelay() {
        return Clock.systemUTC().instant().isBefore(backoffEndTime);
    }

    private synchronized void startNewRequestsIfNeeded() {
        if (messageBuffer.size() + NUM_MESSAGES_PER_REQUEST <= maxQueueSize) {
            if (!longPollRequestInFlight.getAndSet(true)) {
                //always have 1 long-polling request in flight, unless messageBuffer is full
                startNewRequest(RequestType.LONG_POLLING);
            }
        }
        int allocatedRequests = manager.getAllocatedInFlightRequestsCount(this);
        while (inFlightLoadBalancedRequests.get() < allocatedRequests) {
            inFlightLoadBalancedRequests.getAndIncrement();
            startNewRequest(RequestType.LOAD_BALANCED);
        }
    }

    private synchronized void addMessagesToBuffer(List<SqsMessage<T>> messages) {
        if (!messages.isEmpty()) {
            messageBuffer.addAll(messages);
            update();
        }
    }


    private synchronized void queueForProcessingIfNeeded() {
        if (!waitingInQueue && !messageBuffer.isEmpty() && remainingPermits.get() > 0 && !isBlockedByBackoffDelay()) {

            //While this consumer is waiting to be processed, it cannot be added to the queue again
            //This helps guarantee fairness so a single consumer doesn't consume all resources
            waitingInQueue = true;

            manager.queueTask(this::processNextMessage, priority, this);
            applyBackoffDelayIfNeeded();
        }
    }

    void applyBackoffDelayIfNeeded() {
        Duration delay = backoffStrategy.getDelayTime(failureAverage.getAverage());
        if (!delay.isZero() && !delay.isNegative()) {
            backoffEndTime = Clock.systemUTC().instant().plus(delay);
            manager.scheduleTask(new UpdateTimerTask(), delay);
        }
    }

    synchronized SqsMessage<T> getNextMessage() {
        SqsMessage<T> message = messageBuffer.pop();
        waitingInQueue = false;
        remainingPermits.decrementAndGet();
        update();
        return message;
    }

    Completable processNextMessage(SqsMessage<T> message) {
        Duration visibilityTimeout = queueAttributes.getVisibilityTimeout();
        MessageAcknowledger<T> acknowledger =
                new MessageAcknowledger<>(sqsQueue, message.getReceiptHandle(), getMessageAutoExpiration(message));
        if (expirationStrategy.isExpired(message, visibilityTimeout)) {
            acknowledger.ignore();
        } else {
            Completable.fromRunnable(() -> handler.handleMessage(message, acknowledger))
                    .andThen(acknowledger.getAckMode())
                    .onErrorReturnItem(AckMode.IGNORE)
                    .subscribe((ackMode) -> {
                        if (ackMode.isSuccessful()) {
                            failureAverage.addData(MESSAGE_SUCCESS);
                        } else {
                            failureAverage.addData(MESSAGE_FAILURE);
                        }
                        if (ackMode == MessageAcknowledger.AckMode.RETRY) {
                            addMessagesToBuffer(Collections.singletonList(message));
                        }
                    });
        }

        acknowledger.getCompletable().subscribe(() -> {
            remainingPermits.incrementAndGet();
            update();
        });
        return acknowledger.getCompletable();
    }

    public enum RequestType {
        LONG_POLLING,
        LOAD_BALANCED
    }

    private Optional<Instant> getMessageAutoExpiration(SqsMessage<T> message) {
        if (autoExpire) {
            Duration visibilityTimeout = queueAttributes.getVisibilityTimeout();
            Instant expirationTime = message.getReceivedTime().plus(visibilityTimeout);
            return Optional.of(expirationTime);
        } else {
            return Optional.empty();
        }
    }

    private void startNewRequest(RequestType requestType) {
        Duration waitTime = getWaitTimeForRequestType(requestType);
        sqsQueue.receiveMessages(NUM_MESSAGES_PER_REQUEST, Optional.of(waitTime))
                .subscribe(new ReceiveMessageHandler(requestType));
    }

    private static Duration getWaitTimeForRequestType(RequestType requestType) {
        if (requestType == RequestType.LOAD_BALANCED) {
            return LOAD_BALANCED_REQUEST_WAIT_TIME;
        } else {
            return MAX_WAIT_TIME;
        }
    }

    public class UpdateTimerTask extends TimerTask {
        @Override
        public void run() {
            update();
        }
    }

    public class ReceiveMessageHandler implements SingleObserver<List<SqsMessage<T>>> {
        RequestType requestType;

        public ReceiveMessageHandler(RequestType requestType) {
            this.requestType = requestType;
        }

        public void updateLoadBalanceRequests(int numMessages) {
            manager.updateAllocatedInFlightRequests(SqsConsumer.this, new LoadBalanceRequestUpdater(numMessages));
        }

        @Override
        public void onSuccess(List<SqsMessage<T>> messages) {
            addMessagesToBuffer(messages);
            updateLoadBalanceRequests(messages.size());
            always();
        }

        @Override
        public void onError(Throwable exception) {
            LOG.error("SQS receive message failed for queue [{}]", sqsQueue.getQueueUrl(), exception);
            always();
        }

        @Override
        public void onSubscribe(Disposable disposable) {
        }

        public void always() {
            if (requestType == RequestType.LONG_POLLING) {
                longPollRequestInFlight.set(false);
            } else {
                inFlightLoadBalancedRequests.decrementAndGet();
            }
            update();
        }
    }

    public class LoadBalanceRequestUpdater implements Update {
        private final int numMessages;

        public LoadBalanceRequestUpdater(int numMessages) {
            this.numMessages = numMessages;
        }

        @Override
        public Action getAction(int oldValue) {
            int totalNumRequests = oldValue + 1;// +1 for the long-polling request
            boolean bufferFull = messageBuffer.size() + totalNumRequests * NUM_MESSAGES_PER_REQUEST > maxQueueSize;
            if (bufferFull) {
                return Action.Decrease;
            } else {
                return loadBalanceStrategy.onReceiveSuccess(numMessages);
            }
        }
    }
}