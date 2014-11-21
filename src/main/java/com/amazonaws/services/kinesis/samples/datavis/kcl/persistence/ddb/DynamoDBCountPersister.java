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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper.FailedBatch;
import com.amazonaws.services.kinesis.samples.datavis.kcl.persistence.CountPersister;
import com.amazonaws.services.kinesis.samples.datavis.model.HttpReferrerPair;
import com.amazonaws.services.kinesis.samples.datavis.model.HttpReferrerPairsCount;
import com.amazonaws.services.kinesis.samples.datavis.model.ReferrerCount;

/**
 * Persists counts to DynamoDB. This uses a separate thread to send counts to DynamoDB to decouple any network latency
 * from affecting the thread we use to update counts.
 */
public class DynamoDBCountPersister implements CountPersister<HttpReferrerPair> {
    private static final Log LOG = LogFactory.getLog(DynamoDBCountPersister.class);

    // Generate UTC timestamps
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private DynamoDBMapper mapper;

    /**
     * This is used to limit the in memory queue. This number is the total counts we could generate for 10 unique
     * resources in 10 minutes if our update interval is 100ms.
     *
     * 10 resources * 10 minutes * 60 seconds * 10 intervals per second = 60,000.
     */
    private static final int MAX_COUNTS_IN_MEMORY = 60000;

    // The queue holds all pending referrer pair counts to be sent to DynamoDB.
    private BlockingQueue<HttpReferrerPairsCount> counts = new LinkedBlockingQueue<>(MAX_COUNTS_IN_MEMORY);

    // The thread to use for sending counts to DynamoDB.
    private Thread dynamoDBSender;

    /**
     * The hostname of this machine. Used to indicate which host updated a set of counts.
     */
    private String hostname;

    /**
     * Create a new persister with a DynamoDBMapper to translate counts to items and send to Amazon DynamoDB.
     *
     * @param mapper Amazon DynamoDB Mapper to use.
     */
    public DynamoDBCountPersister(DynamoDBMapper mapper) {
        if (mapper == null) {
            throw new NullPointerException("mapper must not be null");
        }
        this.mapper = mapper;
    }

    @Override
    public void initialize() {
        // Resolve our hostname so we can tag the counts this persister produces.
        hostname = resolveHostname();

        // This thread is responsible for draining the queue of new counts and sending them in batches to DynamoDB
        dynamoDBSender = new Thread() {

            @Override
            public void run() {
                // Create a reusable buffer to drain our queue into.
                List<HttpReferrerPairsCount> buffer = new ArrayList<>(MAX_COUNTS_IN_MEMORY);

                // Continuously attempt to drain the queue and send counts to DynamoDB until this thread is interrupted
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        // Drain anything that's in the queue to the buffer and write the items to DynamoDB
                        sendQueueToDynamoDB(buffer);
                        // We wait for an empty queue before checkpointing. Notify that thread when we're empty in
                        // case it is waiting.
                        synchronized(counts) {
                            if (counts.isEmpty()) {
                                counts.notify();
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.error("Thread that handles persisting counts to DynamoDB was interrupted. Counts will no longer be persisted!",
                                e);
                        return;
                    } finally {
                        // Clear the temporary buffer to release references to persisted counts
                        buffer.clear();
                    }
                }
            }
        };
        dynamoDBSender.setDaemon(true);
        dynamoDBSender.start();
    }

    @Override
    public void persist(Map<HttpReferrerPair, Long> objectCounts) {
        if (objectCounts.isEmpty()) {
            // short circuit to avoid creating a map when we have no objects to persist
            return;
        }

        // Use a local collection to batch writing the new counts into the queue. This will allow the queue drainer
        // to remain simple as it doesn't have to account for less than full batches.

        // We map resource to pair counts so we can easily look up a resource and add referrer counts to it
        Map<String, HttpReferrerPairsCount> countMap = new HashMap<>();

        for (Map.Entry<HttpReferrerPair, Long> count : objectCounts.entrySet()) {
            // Check for an existing counts for this resource
            HttpReferrerPair pair = count.getKey();
            HttpReferrerPairsCount pairCount = countMap.get(pair.getResource());
            if (pairCount == null) {
                // Create a new pair if this resource hasn't been seen yet in this batch
                pairCount = new HttpReferrerPairsCount();
                pairCount.setResource(pair.getResource());
                pairCount.setTimestamp(Calendar.getInstance(UTC).getTime());
                pairCount.setReferrerCounts(new ArrayList<ReferrerCount>());
                pairCount.setHost(hostname);
                countMap.put(pair.getResource(), pairCount);
            }

            // Add referrer to list of refcounts for this resource and time
            ReferrerCount refCount = new ReferrerCount();
            refCount.setReferrer(pair.getReferrer());
            refCount.setCount(count.getValue());
            pairCount.getReferrerCounts().add(refCount);
        }

        // Top N calculation for this interval
        // By sorting the referrer counts list in descending order the consumer of the count data can choose their own
        // N.
        for (HttpReferrerPairsCount count : countMap.values()) {
            Collections.sort(count.getReferrerCounts(), new Comparator<ReferrerCount>() {
                @Override
                public int compare(ReferrerCount c1, ReferrerCount c2) {
                    if (c2.getCount() > c1.getCount()) {
                        return 1;
                    } else if (c1.getCount() == c2.getCount()) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
            });
        }
        counts.addAll(countMap.values());
    }

    /**
     * We will block until the entire queue of counts has been drained.
     */
    @Override
    public void checkpoint() throws InterruptedException {
        // We need to make sure all counts are flushed to DynamoDB before we return successfully.
        if (dynamoDBSender.isAlive()) {
            // If the DynamoDB thread is running wait until our counts queue is empty
            synchronized(counts) {
                while (!counts.isEmpty()) {
                    counts.wait();
                }
                // All the counts we currently know about have been persisted. It is now safe to return from this blocking call.
            }
        } else {
            throw new IllegalStateException("DynamoDB persister thread is not running. Counts are not persisted and we should not checkpoint!");
        }
    }

    /**
     * Drain the queue of pending counts into the provided buffer and write those counts to DynamoDB. This blocks until
     * data is available in the queue.
     *
     * @param buffer A reusable buffer with sufficient space to drain the entire queue if necessary. This is provided as
     *        an optimization to avoid allocating a new buffer every interval.
     * @throws InterruptedException Thread interrupted while waiting for new data to arrive in the queue.
     */
    protected void sendQueueToDynamoDB(List<HttpReferrerPairsCount> buffer) throws InterruptedException {
        // Block while waiting for data
        buffer.add(counts.take());
        // Drain as much of the queue as we can.
        // DynamoDBMapper will handle splitting the batch sizes for us.
        counts.drainTo(buffer);
        try {
            long start = System.nanoTime();
            // Write the contents of the buffer as items to our table
            List<FailedBatch> failures = mapper.batchWrite(buffer, Collections.emptyList());
            long end = System.nanoTime();
            LOG.info(String.format("%d new counts sent to DynamoDB in %dms",
                    buffer.size(),
                    TimeUnit.NANOSECONDS.toMillis(end - start)));

            for (FailedBatch failure : failures) {
                LOG.warn("Error sending count batch to DynamoDB. This will not be retried!", failure.getException());
            }
        } catch (Exception ex) {
            LOG.error("Error sending new counts to DynamoDB. The some counts may not be persisted.", ex);
        }
    }

    /**
     * Resolve the hostname of the machine executing this code.
     *
     * @return The hostname, or "unknown", if one cannot be determined.
     */
    private String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            LOG.warn("Unable to determine hostname. Counts from this worker will be registered as counted by 'unknown'!",
                    uhe);
        }
        return "unknown";
    }
}
