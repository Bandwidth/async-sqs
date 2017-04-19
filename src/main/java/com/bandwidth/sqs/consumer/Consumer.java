package com.bandwidth.sqs.consumer;

import static java.util.Objects.requireNonNull;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.bandwidth.sqs.consumer.acknowledger.DefaultMessagePublisher;
import com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger;
import com.bandwidth.sqs.consumer.acknowledger.MessagePublisher;
import com.bandwidth.sqs.consumer.strategy.expiration.ExpirationStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.DefaultLoadBalanceStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy;
import com.bandwidth.sqs.consumer.strategy.loadbalance.LoadBalanceStrategy.Action;
import com.bandwidth.sqs.consumer.strategy.backoff.BackoffStrategy;
import com.bandwidth.sqs.consumer.handler.ConsumerHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.reactivex.Completable;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.CompletableSubject;

import static com.bandwidth.sqs.consumer.acknowledger.MessageAcknowledger.AckMode;

public class Consumer {
    public static final int NUM_MESSAGES_PER_REQUEST = 10;
    public static final int LOAD_BALANCED_REQUEST_WAIT_TIME_SECONDS = 1;
    public static final int MAX_WAIT_TIME_SECONDS = 20;
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    static final long MESSAGE_SUCCESS = 0;
    static final long MESSAGE_FAILURE = 1;

    private static final int TIME_WINDOW_MIN_COUNT = 10;

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final String queueUrl;
    private final AtomicInteger maxPermits;
    private final AtomicInteger remainingPermits;
    private final ConsumerHandler<Message> handler;

    private final int maxQueueSize;
    private final BackoffStrategy backoffStrategy;
    private final ConsumerManager manager;
    private final ExpirationStrategy expirationStrategy;
    private final AtomicInteger inFlightLoadBalancedRequests = new AtomicInteger(0);
    private final AtomicBoolean longPollRequestInFlight = new AtomicBoolean(false);
    private final CompletableSubject shutdownCompletable = CompletableSubject.create();
    private final Disposable permitChangeDisposable;

    private ArrayDeque<TimedMessage> messageBuffer = new ArrayDeque<>();
    private boolean waitingInQueue = false;
    private TimeWindowAverage failureAverage = null;
    private Instant backoffEndTime = Instant.EPOCH;
    private LoadBalanceStrategy loadBalanceStrategy = new DefaultLoadBalanceStrategy();
    private boolean shuttingDown = false;

    /**
     * Adds a consumer for a specific SQS Queue. Once a consumer is started, the handler will be called
     * from a thread-pool to process messages. It is safe to use blocking calls in the handler as
     * this will not impact performance if a sufficient number of `workerThreads` are configured in the ConsumerManager.
     * Only one consumer is normally needed per SQS Queue. A single long-polling request is always in-flight
     * for each consumer in addition to the load-balanced requests configured in the `ConsumerManager`.
     */
    public Consumer(ConsumerBuilder builder) {
        this.handler = requireNonNull(builder.consumerHandler);
        this.backoffStrategy = requireNonNull(builder.backoffStrategy);
        this.manager = requireNonNull(builder.consumerManager);
        this.expirationStrategy = requireNonNull(builder.expirationStrategy);

        this.queueUrl = builder.queueUrl;
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

    public String getQueueUrl() {
        return queueUrl;
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

    /**
     * Consumer cannot be started after it has been shutdown
     */
    public void start() {
        update();
    }

    public void update() {
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
    public void shutdown() {
        shutdown(DEFAULT_SHUTDOWN_TIMEOUT);
    }

    /**
     * Shuts down the consumer, and blocks until the shutdown completes.
     * Throws a timeout exception if it fails to shutdown in the timeout specified
     */
    public void shutdown(Duration timeout) {
        permitChangeDisposable.dispose();
        shutdownAsync()
                .timeout(timeout.getSeconds(), TimeUnit.SECONDS)
                .blockingAwait();
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

    void setMessageBuffer(ArrayDeque<TimedMessage> messageBuffer) {
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
                startNewRequest(RequestType.LongPolling);
            }
        }

        int allocatedRequests = manager.getAllocatedInFlightRequestsCount(this);

        while (inFlightLoadBalancedRequests.get() < allocatedRequests) {
            inFlightLoadBalancedRequests.getAndIncrement();
            startNewRequest(RequestType.LoadBalanced);
        }
    }

    private synchronized void addMessagesToBuffer(List<TimedMessage> messages) {
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

            manager.queueTask(this::processNextMessage);
            checkIfBackoffDelayNeeded();
        }
    }

    void checkIfBackoffDelayNeeded() {
        Duration delay = backoffStrategy.getDelayTime(failureAverage.getAverage());
        if (!delay.isZero() && !delay.isNegative()) {
            backoffEndTime = Clock.systemUTC().instant().plus(delay);
            manager.scheduleTask(new UpdateTimerTask(), delay);
        }
    }

    private synchronized TimedMessage getNextMessage() {
        TimedMessage timedMessage = messageBuffer.pop();
        waitingInQueue = false;

        boolean expired = expirationStrategy.isExpired(timedMessage);

        if (expired) {
            update();
            return null;
        } else {
            remainingPermits.decrementAndGet();
            update();
            return timedMessage;
        }
    }

    void processNextMessage() {

        TimedMessage timedMessage = getNextMessage();
        if (timedMessage == null) {
            return;
        }
        Message message = timedMessage.getMessage();

        MessagePublisher<Message> publisher = new DefaultMessagePublisher(manager.getSqsClient());
        MessageAcknowledger<Message> acknowledger =
                new MessageAcknowledger<>(manager.getSqsClient(), queueUrl, message.getReceiptHandle(), publisher);

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
                        addMessagesToBuffer(Collections.singletonList(timedMessage));
                    }
                });

        //TODO: add a timeout here to guarantee the permit is released (future PR)
        acknowledger.getCompletable().doFinally(() -> {
            remainingPermits.incrementAndGet();
            update();
        }).subscribe();
    }

    private void startNewRequest(RequestType requestType) {
        ReceiveMessageRequest request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(NUM_MESSAGES_PER_REQUEST);
        if (requestType == RequestType.LongPolling) {
            //long polling waits for as long as possible
            request = request.withWaitTimeSeconds(MAX_WAIT_TIME_SECONDS);
        } else {
            //load balanced requests wait the minimum amount of time that guarantees they get all requests (1 second)
            request = request.withWaitTimeSeconds(LOAD_BALANCED_REQUEST_WAIT_TIME_SECONDS);
        }
        manager.getSqsClient().receiveMessage(request)
                .subscribe(new ReceiveMessageHandler(requestType));
    }

    public enum RequestType {
        LongPolling,
        LoadBalanced
    }

    public class UpdateTimerTask extends TimerTask {
        @Override
        public void run() {
            update();
        }
    }

    public class ReceiveMessageHandler implements SingleObserver<ReceiveMessageResult> {
        RequestType requestType;

        public ReceiveMessageHandler(RequestType requestType) {
            this.requestType = requestType;
        }

        public void updateLoadBalanceRequests(int numMessages) {
            manager.updateAllocatedInFlightRequests(Consumer.this, new LoadBalanceRequestUpdater(numMessages));
        }

        @Override
        public void onSuccess(ReceiveMessageResult result) {
            List<TimedMessage> messages = result.getMessages().stream()
                    .map((message) -> TimedMessage.builder()
                            .message(message)
                            .build()
                    ).collect(Collectors.toList());
            addMessagesToBuffer(messages);
            updateLoadBalanceRequests(result.getMessages().size());
            always();
        }

        @Override
        public void onError(Throwable exception) {
            LOG.error("SQS receive message failed for queue [{}]", exception, queueUrl);
            always();
        }

        @Override
        public void onSubscribe(Disposable disposable) {
        }

        public void always() {
            if (requestType == RequestType.LongPolling) {
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