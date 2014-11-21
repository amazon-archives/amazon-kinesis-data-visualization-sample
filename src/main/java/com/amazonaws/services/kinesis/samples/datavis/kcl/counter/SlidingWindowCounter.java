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

import java.util.Map;

/**
 * Computes a total count of occurrences over a moving window. All calls to increment will be added to the current
 * internal bucket. As the window advances the last bucket in the window will be removed.
 */
public class SlidingWindowCounter<ObjectType> {

    private BucketBasedCounter<ObjectType> counter;

    private int windowSize;
    private int headBucket;
    private int tailBucket;
    // Keep track of the total window advances so we can answer the question: Is this window full?
    private int totalAdvances;

    public SlidingWindowCounter(int windowSize) {
        if (windowSize < 1) {
            throw new IllegalArgumentException("windowSize must be >= 1");
        }
        this.windowSize = windowSize;

        counter = new BucketBasedCounter<>(windowSize);
        headBucket = 0;
        tailBucket = getNextBucket(headBucket);
    }

    /**
     * Determine which bucket comes "after" a given bucket. This handles the edge case where the bucket provided is at
     * the end of the list of buckets.
     * 
     * @param bucket The index of the bucket to start at.
     * @return The bucket that is logically after the given bucket.
     */
    private int getNextBucket(int bucket) {
        return (bucket + 1) % windowSize;
    }

    /**
     * Increment the count for an object in the current bucket by 1.
     * 
     * @param obj Object whose count should be incremented.
     */
    public void increment(ObjectType obj) {
        counter.increment(obj, headBucket);
    }

    /**
     * Get the counts for all objects across all buckets.
     * 
     * @return A mapping of ObjectType -> total count across all buckets.
     */
    public Map<ObjectType, Long> getCounts() {
        return counter.getCounts();
    }

    /**
     * Advance the window "one bucket". This will remove the oldest bucket and any count stored in it.
     */
    public void advanceWindow() {
        counter.clearBucket(tailBucket);

        headBucket = tailBucket;
        tailBucket = getNextBucket(headBucket);

        if (!isWindowFull()) {
            // Only increment if our window has not yet filled up.
            totalAdvances++;
        }
    }

    /**
     * Check if we've advanced our window enough times to have completely filled all buckets.
     * 
     * @return {@code true} if the window is full.
     */
    public boolean isWindowFull() {
        return windowSize <= totalAdvances;
    }

    /**
     * @see BucketBasedCounter#pruneEmptyObjects()
     */
    public void pruneEmptyObjects() {
        counter.pruneEmptyObjects();
    }
}
