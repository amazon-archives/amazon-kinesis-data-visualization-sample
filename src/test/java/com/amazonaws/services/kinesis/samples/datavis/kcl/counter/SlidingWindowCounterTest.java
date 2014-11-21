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

public class SlidingWindowCounterTest {
    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_newWindow_WHEN_windowSizeLessThanOne_THEN_throwException() {
        new SlidingWindowCounter<>(0);
    }

    @Test
    public void GIVEN_newWindow_WHEN_increment_THEN_getCountsReturnsCorrectCount() throws Exception {
        SlidingWindowCounter<String> counter = new SlidingWindowCounter<>(2);

        counter.increment("a");
        counter.increment("a");
        counter.increment("b");

        Map<String, Long> counts = counter.getCounts();

        assertEquals(2, counts.size());
        assertEquals(new Long(2), counts.get("a"));
        assertEquals(new Long(1), counts.get("b"));
    }

    @Test
    public void GIVEN_windowInitiallyFilling_WHEN_advanceWindow_THEN_countsRemainTheSame() throws Exception {
        SlidingWindowCounter<String> counter = new SlidingWindowCounter<>(2);
        counter.increment("a");
        Map<String, Long> counts = counter.getCounts();

        counter.advanceWindow();

        Map<String, Long> newCounts = counter.getCounts();

        assertEquals(counts, newCounts);
    }

    @Test
    public void GIVEN_fullWindow_WHEN_advanceWindow_THEN_countsDecrementByFirstBucketCount() throws Exception {
        SlidingWindowCounter<String> counter = new SlidingWindowCounter<>(2);
        counter.increment("a");
        counter.increment("b");
        counter.advanceWindow();
        counter.increment("a");
        counter.increment("b");
        // Window now has 2 full buckets

        Map<String, Long> counts = counter.getCounts();

        assertEquals(2, counts.size());
        assertEquals(new Long(2), counts.get("a"));
        assertEquals(new Long(2), counts.get("b"));

        counter.advanceWindow();

        Map<String, Long> newCounts = counter.getCounts();

        // We should still have 2 distinct values
        assertEquals(2, newCounts.size());
        // Counts should be down to just 1
        assertEquals(new Long(1), newCounts.get("a"));
        assertEquals(new Long(1), newCounts.get("b"));
    }

    @Test
    public void GIVEN_fullWindow_WHEN_isWindowFull_THEN_returnTrue() {
        SlidingWindowCounter<String> counter = new SlidingWindowCounter<>(1);
        assertFalse(counter.isWindowFull());

        counter.advanceWindow();
        assertTrue(counter.isWindowFull());

        counter.advanceWindow();
        assertTrue(counter.isWindowFull());
    }

    @Test
    public void GIVEN_lastDataPoint_WHEN_windowAdvances_THEN_getCountsReturnZeroForDataWithNoCounts() {
        SlidingWindowCounter<String> counter = new SlidingWindowCounter<>(1);

        counter.increment("a");
        counter.advanceWindow();

        Map<String, Long> counts = counter.getCounts();

        assertEquals(1, counts.size());
        assertEquals(new Long(0), counts.get("a"));
    }

    @Test
    public void GIVEN_objectWithNoCounts_WHEN_pruneEmptyObjects_THEN_getCountsNoLongerReturnsObject() {
        SlidingWindowCounter<String> counter = new SlidingWindowCounter<>(1);

        counter.increment("a");
        counter.advanceWindow();

        Map<String, Long> counts = counter.getCounts();

        assertEquals(1, counts.size());
        assertEquals(new Long(0), counts.get("a"));

        counter.pruneEmptyObjects();

        assertTrue(counter.getCounts().isEmpty());
    }
}
