/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.datavis.kcl.persistence.ddb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.kinesis.samples.datavis.model.HttpReferrerPair;
import com.amazonaws.services.kinesis.samples.datavis.model.HttpReferrerPairsCount;
import com.amazonaws.services.kinesis.samples.datavis.model.ReferrerCount;

public class DynamoDBCountPersisterTest {

    private DynamoDBCountPersister persister;
    private DynamoDBMapper mapper;

    // All methods in this test class timeout after 1 second to prevent a bug when dequeuing from the blocking queue to
    // prevent our test suite from hanging.
    @Rule
    public TestRule globalTimeout = new Timeout(1000);

    @Before
    public void init() {
        mapper = mock(DynamoDBMapper.class);
        persister = new DynamoDBCountPersister(mapper);
    }

    @Test
    public void GIVEN_persistCalledWithCounts_WHEN_sendQueueToDynamoDB_THEN_mapperCalledWithCounts()
        throws InterruptedException {

        final HttpReferrerPair pair = new HttpReferrerPair("a", "b");
        final long count = 1L;
        persister.persist(Collections.singletonMap(pair, count));

        persister.sendQueueToDynamoDB(new ArrayList<HttpReferrerPairsCount>());

        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<List<HttpReferrerPairsCount>> pairsCountCaptor = ArgumentCaptor.forClass((Class) List.class);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<List<HttpReferrerPairsCount>> ignoredCaptor = ArgumentCaptor.forClass((Class) List.class);

        // Capture the arguments passed to the mapper when persisting counts to DynamoDB
        verify(mapper).batchWrite(pairsCountCaptor.capture(), ignoredCaptor.capture());
        List<HttpReferrerPairsCount> receivedPairsCounts = pairsCountCaptor.getValue();
        assertEquals(1, receivedPairsCounts.size());
        HttpReferrerPairsCount receivedPairCount = receivedPairsCounts.get(0);
        assertEquals(pair.getResource(), receivedPairCount.getResource());
        assertEquals(1, receivedPairCount.getReferrerCounts().size());
        ReferrerCount receivedReferrerCount = receivedPairCount.getReferrerCounts().get(0);
        assertEquals(pair.getReferrer(), receivedReferrerCount.getReferrer());
        assertEquals(count, receivedReferrerCount.getCount());
    }

    @Test
    public void GIVEN_persistCalledWithTwoUniqueResources_WHEN_sendQueueToDynamoDB_THEN_refCountsAreOrderedDescending()
        throws InterruptedException {
        // Create pairs with different referrers and counts.
        String resource = "a";
        Map<HttpReferrerPair, Long> counts = new HashMap<>();
        counts.put(new HttpReferrerPair(resource, "b"), 10L);
        counts.put(new HttpReferrerPair(resource, "c"), 7L);
        counts.put(new HttpReferrerPair(resource, "d"), 15L);
        counts.put(new HttpReferrerPair(resource, "e"), 20L);
        counts.put(new HttpReferrerPair(resource, "f"), 20L);

        // Persist the counts
        persister.persist(counts);

        // Trigger the flush to DynamoDB.
        persister.sendQueueToDynamoDB(new ArrayList<HttpReferrerPairsCount>());

        // Capture the arguments passed to the mapper when persisting counts to DynamoDB
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<List<HttpReferrerPairsCount>> pairsCountCaptor = ArgumentCaptor.forClass((Class) List.class);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<List<HttpReferrerPairsCount>> ignoredCaptor = ArgumentCaptor.forClass((Class) List.class);
        verify(mapper).batchWrite(pairsCountCaptor.capture(), ignoredCaptor.capture());

        List<HttpReferrerPairsCount> receivedPairsCounts = pairsCountCaptor.getValue();
        assertEquals(1, receivedPairsCounts.size());
        HttpReferrerPairsCount receivedPairCount = receivedPairsCounts.get(0);
        assertEquals(resource, receivedPairCount.getResource());
        assertEquals(5, receivedPairCount.getReferrerCounts().size());

        // Make sure the counts are descending
        long lastCount = Long.MAX_VALUE;
        for (ReferrerCount refCount : receivedPairCount.getReferrerCounts()) {
            assertTrue("Count did not decrease from the last one seen. " + lastCount + " is not > "
                    + refCount.getCount(),
                    lastCount >= refCount.getCount());
            lastCount = refCount.getCount();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void GIVEN_initializedPersister_WHEN_persist_THEN_countsPersistedInBatch() throws InterruptedException {
        persister.initialize();

        final HttpReferrerPair pair = new HttpReferrerPair("a", "b");
        final long count = 1L;
        persister.persist(Collections.singletonMap(pair, count));

        // Wait for the persister thread to pick up the new counts
        Thread.sleep(100);

        // Verify the counts were sent to DynamoDB
        verify(mapper).batchWrite(Mockito.any(List.class), Mockito.any(List.class));
    }

    @Test
    public void GIVEN_initializedPersister_WHEN_checkpoint_THEN_checkpointReturnsSuccessfully()
        throws InterruptedException {
        persister.initialize();

        persister.checkpoint();
    }
}
