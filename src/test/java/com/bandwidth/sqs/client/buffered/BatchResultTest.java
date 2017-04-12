package com.bandwidth.sqs.client.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import static java.util.Collections.singletonList;

import com.amazonaws.services.sqs.model.BatchResultErrorEntry;

import org.junit.Test;

import java.util.Collection;

public class BatchResultTest {

    private static final String SUCCESS_DATA = "success";

    @Test
    public void testAll() {
        Collection<String> successEntries = singletonList(SUCCESS_DATA);
        Collection<BatchResultErrorEntry> errorEntries = singletonList(new BatchResultErrorEntry());
        BatchResult<String> results = new BatchResult<>(successEntries, errorEntries);

        assertThat(results.getSuccessResults()).isEqualTo(successEntries);
        assertThat(results.getErrorResults()).isEqualTo(errorEntries);
    }
}
