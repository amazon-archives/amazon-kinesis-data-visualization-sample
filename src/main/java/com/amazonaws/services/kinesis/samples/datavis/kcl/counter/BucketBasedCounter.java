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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides a way to count the occurrences of objects across a number of discrete "buckets". These buckets usually
 * represent a time period such as 1 second.
 */
public class BucketBasedCounter<ObjectType> {
    private Map<ObjectType, long[]> objectCounts;
    private int maxBuckets;

    /**
     * Create a new counter with a fixed number of buckets.
     * 
     * @param maxBuckets Total buckets this counter will use.
     */
    public BucketBasedCounter(int maxBuckets) {
        if (maxBuckets < 1) {
            throw new IllegalArgumentException("maxBuckets must be >= 1");
        }
        objectCounts = new HashMap<>();
        this.maxBuckets = maxBuckets;
    }

    /**
     * Increment the count of the object for a specific bucket index.
     * 
     * @param obj Object whose count should be updated.
     * @param bucket Index of bucket to increment.
     * @return The new count for that object at the bucket index provided.
     */
    public long increment(ObjectType obj, int bucket) {
        long[] counts = objectCounts.get(obj);
        if (counts == null) {
            counts = new long[maxBuckets];
            objectCounts.put(obj, counts);
        }
        return ++counts[bucket];
    }

    /**
     * Computes the total count for all objects across all buckets.
     * 
     * @return A mapping of object to total count across all buckets.
     */
    public Map<ObjectType, Long> getCounts() {
        Map<ObjectType, Long> count = new HashMap<>();

        for (Map.Entry<ObjectType, long[]> entry : objectCounts.entrySet()) {
            count.put(entry.getKey(), calculateTotal(entry.getValue()));
        }

        return count;
    }

    /**
     * Calculates the sum total of occurrences across all bucket counts.
     * 
     * @param counts A set of count buckets from the same object.
     * @return The sum of all counts in the buckets.
     */
    private long calculateTotal(long[] counts) {
        long total = 0;
        for (long count : counts) {
            total += count;
        }
        return total;
    }

    /**
     * Remove any objects whose buckets total 0.
     */
    public void pruneEmptyObjects() {
        List<ObjectType> toBePruned = new ArrayList<>();
        for (Map.Entry<ObjectType, long[]> entry : objectCounts.entrySet()) {
            // Remove objects whose total counts are 0
            if (calculateTotal(entry.getValue()) == 0) {
                toBePruned.add(entry.getKey());
            }
        }
        for (ObjectType prune : toBePruned) {
            objectCounts.remove(prune);
        }
    }

    /**
     * Clears all object counts for the given bucket. If you wish to remove objects that no longer have any counts in
     * any bucket use {@link #pruneEmptyObjects()}.
     * 
     * @param bucket The index of the bucket to clear.
     */
    public void clearBucket(int bucket) {
        for (long[] counts : objectCounts.values()) {
            counts[bucket] = 0;
        }
    }
}
