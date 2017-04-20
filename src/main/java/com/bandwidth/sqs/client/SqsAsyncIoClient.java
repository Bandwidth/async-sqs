package com.bandwidth.sqs.client;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.bandwidth.sqs.publisher.MessagePublisher;

import java.time.Duration;
import java.util.Optional;

import io.reactivex.Single;

public interface SqsAsyncIoClient extends MessagePublisher<Message>{

    Single<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request);

    Single<DeleteMessageResult> deleteMessage(DeleteMessageRequest request);

    Single<String> publishMessage(Message message, String queueUrl, Optional<Duration> delay);

    Single<ChangeMessageVisibilityResult> changeMessageVisibility(ChangeMessageVisibilityRequest request);

    Single<DeleteMessageBatchResult> deleteMessageBatch(DeleteMessageBatchRequest request);

    Single<SendMessageBatchResult> sendMessageBatch(SendMessageBatchRequest request);

    Single<ChangeMessageVisibilityBatchResult>
        changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request);
}
