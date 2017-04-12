package com.bandwidth.sqs.client.buffered;

import com.amazonaws.services.sqs.model.BatchResultErrorEntry;

import java.util.Collection;

public class BatchResult<T> {
    public Collection<T> successResults;
    public Collection<BatchResultErrorEntry> errorResults;

    public BatchResult(Collection<T> successResults, Collection<BatchResultErrorEntry> errorResults) {
        this.successResults = successResults;
        this.errorResults = errorResults;
    }

    public Collection<T> getSuccessResults() {
        return successResults;
    }

    public Collection<BatchResultErrorEntry> getErrorResults() {
        return errorResults;
    }
}