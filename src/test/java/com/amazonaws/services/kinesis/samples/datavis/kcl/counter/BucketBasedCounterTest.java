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

package com.amazonaws.services.kinesis.samples.datavis.kcl.counter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

public class BucketBasedCounterTest {

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_newCounter_WHEN_maxBucketsLessThanOne_THEN_throwException() {
        new BucketBasedCounter<>(0);
    }

    @Test
    public void GIVEN_noIncrements_WHEN_getCounts_THEN_returnEmptyMap() {
        BucketBasedCounter<String> counter = new BucketBasedCounter<>(2);

        Map<String, Long> counts = counter.getCounts();

        assertTrue(counts.isEmpty());
    }

    @Test
    public void GIVEN_incrementOnTwoDistinctObjects_WHEN_getCounts_THEN_returnBothDistinctCounts() {
        BucketBasedCounter<String> counter = new BucketBasedCounter<>(2);
        counter.increment("a", 0);
        counter.increment("b", 0);

        Map<String, Long> counts = counter.getCounts();

        assertEquals(2, counts.size());
        assertEquals(new Long(1), counts.get("a"));
        assertEquals(new Long(1), counts.get("b"));
    }

    @Test
    public void GIVEN_objectHasExistingCounts_WHEN_increment_THEN_returnExistingCountPlusOne() {
        BucketBasedCounter<String> counter = new BucketBasedCounter<>(2);
        counter.increment("a", 0);

        Map<String, Long> counts = counter.getCounts();

        counter.increment("a", 0);

        Map<String, Long> counts2 = counter.getCounts();

        assertTrue(counts.get("a") + 1 == counts2.get("a"));
    }

    @Test
    public void GIVEN_objectWithCountsInASingleBucket_WHEN_clearBucket_THEN_countsEqualZero() {
        BucketBasedCounter<String> counter = new BucketBasedCounter<>(2);

        counter.increment("a", 0);

        counter.clearBucket(0);

        assertEquals(new Long(0), counter.getCounts().get("a"));
    }

    @Test
    public void GIVEN_objectHasCountsAcrossMultipleBuckets_WHEN_clearBucket_THEN_returnCountsForOtherBuckets() {
        BucketBasedCounter<String> counter = new BucketBasedCounter<>(2);

        counter.increment("a", 0);
        counter.increment("a", 1);

        assertEquals(new Long(2), counter.getCounts().get("a"));

        counter.clearBucket(0);

        assertEquals(new Long(1), counter.getCounts().get("a"));
    }

    @Test
    public void GIVEN_objectHasNoCounts_WHEN_pruneEmptyObjects_THEN_getCountsDoesNotReturnObject() {
        BucketBasedCounter<String> counter = new BucketBasedCounter<>(1);
        counter.increment("a", 0);
        counter.clearBucket(0);

        counter.pruneEmptyObjects();

        assertFalse(counter.getCounts().containsKey("a"));
    }
}
