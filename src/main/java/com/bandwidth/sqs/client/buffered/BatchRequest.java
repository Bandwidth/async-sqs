package com.bandwidth.sqs.client.buffered;

import java.util.Collection;


public class BatchRequest<T> {
    private final String queueUrl;
    private final Collection<T> batchRequestEntries;

    public BatchRequest(String queueUrl, Collection<T> batchRequestEntries) {
        this.queueUrl = queueUrl;
        this.batchRequestEntries = batchRequestEntries;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public Collection<T> getBatchRequestEntries() {
        return batchRequestEntries;
    }
}
