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

package com.amazonaws.services.kinesis.samples.datavis.kcl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samples.datavis.kcl.counter.SlidingWindowCounter;
import com.amazonaws.services.kinesis.samples.datavis.kcl.persistence.CountPersister;
import com.amazonaws.services.kinesis.samples.datavis.kcl.timing.Clock;
import com.amazonaws.services.kinesis.samples.datavis.kcl.timing.NanoClock;
import com.amazonaws.services.kinesis.samples.datavis.kcl.timing.Timer;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Computes a map of (HttpReferrerPair -> count(pair)) over a fixed range of time. Counts are computed at the intervals
 * provided.
 *
 * @param <T> The type of records this processor is capable of counting.
 */
public class CountingRecordProcessor<T> implements IRecordProcessor {
    private static final Log LOG = LogFactory.getLog(CountingRecordProcessor.class);

    // Lock to use for our timer
    private static final Clock NANO_CLOCK = new NanoClock();
    // The timer to schedule checkpoints with
    private Timer checkpointTimer = new Timer(NANO_CLOCK);

    // Our JSON object mapper for deserializing records
    private final ObjectMapper JSON;

    // Interval to calculate distinct counts across
    private int computeIntervalInMillis;
    // Total range to consider counts when calculating totals
    private int computeRangeInMillis;

    // Counter for keeping track of counts per interval.
    private SlidingWindowCounter<T> counter;

    // The shard this processor is processing
    private String kinesisShardId;

    // We schedule count updates at a fixed rate (computeIntervalInMillis) on a separate thread
    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    // This is responsible for persisting our counts every interval
    private CountPersister<T> persister;

    private CountingRecordProcessorConfig config;

    // The type of record we expect to receive as JSON
    private Class<T> recordType;

    /**
     * Create a new processor.
     *
     * @param config Configuration for this record processor.
     * @param recordType The type of record we expect to receive as a UTF-8 JSON string.
     * @param persister Counts will be persisted with this persister.
     * @param computeRangeInMillis Range to compute distinct counts across
     * @param computeIntervalInMillis Interval between computing total count for the overall time range.
     */
    public CountingRecordProcessor(CountingRecordProcessorConfig config,
            Class<T> recordType,
            CountPersister<T> persister,
            int computeRangeInMillis,
            int computeIntervalInMillis) {
        if (config == null) {
            throw new NullPointerException("config must not be null");
        }
        if (recordType == null) {
            throw new NullPointerException("recordType must not be null");
        }
        if (persister == null) {
            throw new NullPointerException("persister must not be null");
        }
        if (computeRangeInMillis <= 0) {
            throw new IllegalArgumentException("computeRangeInMillis must be > 0");
        }
        if (computeIntervalInMillis <= 0) {
            throw new IllegalArgumentException("computeIntervalInMillis must be > 0");
        }
        if (computeRangeInMillis % computeIntervalInMillis != 0) {
            throw new IllegalArgumentException("compute range must be evenly divisible by compute interval to support "
                    + "accurate intervals");
        }

        this.config = config;
        this.recordType = recordType;
        this.persister = persister;
        this.computeRangeInMillis = computeRangeInMillis;
        this.computeIntervalInMillis = computeIntervalInMillis;

        // Create an object mapper to deserialize records that ignores unknown properties
        JSON = new ObjectMapper();
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void initialize(String shardId) {
        kinesisShardId = shardId;
        resetCheckpointAlarm();

        persister.initialize();

        // Create a sliding window whose size is large enough to hold an entire range of individual interval counts.
        counter = new SlidingWindowCounter<>((int) (computeRangeInMillis / computeIntervalInMillis));

        // Create a scheduled task that runs every computeIntervalInMillis to compute and
        // persist the counts.
        scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // Synchronize on the counter so we stop advancing the interval while we're checkpointing
                synchronized (counter) {
                    try {
                        advanceOneInterval();
                    } catch (Exception ex) {
                        LOG.warn("Error advancing sliding window one interval (" + computeIntervalInMillis
                                + "ms). Skipping this interval.", ex);
                    }
                }
            }
        },
                TimeUnit.SECONDS.toMillis(config.getInitialWindowAdvanceDelayInSeconds()),
                computeIntervalInMillis,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Advance the internal sliding window counter one interval. This will invoke our count persister if the window is
     * full.
     */
    protected void advanceOneInterval() {
        Map<T, Long> counts = null;
        synchronized (counter) {
            // Only persist the counts if we have a full range of data to report. We don't want partial
            // counts each time the process starts.
            if (shouldPersistCounts()) {
                counts = counter.getCounts();
                counter.pruneEmptyObjects();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("We have not collected enough interval samples to calculate across the "
                            + "entire range from shard %s. Skipping this interval.", kinesisShardId));
                }
            }
            // Advance the window "1 tick"
            counter.advanceWindow();
        }
        // Persist the counts if we have a full range
        if (counts != null) {
            persister.persist(counts);
        }
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record r : records) {
            // Deserialize each record as an UTF-8 encoded JSON String of the type provided
            T pair;
            try {
                pair = JSON.readValue(r.getData().array(), recordType);
            } catch (IOException e) {
                LOG.warn("Skipping record. Unable to parse record into HttpReferrerPair. Partition Key: "
                        + r.getPartitionKey() + ". Sequence Number: " + r.getSequenceNumber(),
                        e);
                continue;
            }
            // Increment the counter for the new pair. This is synchronized because there is another thread reading from
            // the counter to compute running totals every interval.
            synchronized (counter) {
                counter.increment(pair);
            }
        }

        // Checkpoint if it's time to!
        if (checkpointTimer.isTimeUp()) {
            // Obtain a lock on the counter to prevent additional counts from being calculated while checkpointing.
            synchronized (counter) {
                checkpoint(checkpointer);
                resetCheckpointAlarm();
            }
        }
    }

    /**
     * We must have collected a full range window worth of samples before we should persist any counts.
     *
     * @return {@code true} if we've collected enough samples to persist a complete count for the entire range.
     */
    private boolean shouldPersistCounts() {
        return counter.isWindowFull();
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);

        scheduledExecutor.shutdown();
        try {
            // Wait for at most 30 seconds for the executor service's tasks to complete
            if (!scheduledExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Failed to properly shut down interval thread pool for calculating interval counts and persisting them. Some counts may not have been persisted.");
            } else {
                // Only checkpoint if we successfully shut down the thread pool
                // Important to checkpoint after reaching end of shard, so we can start processing data from child
                // shards.
                if (reason == ShutdownReason.TERMINATE) {
                    synchronized (counter) {
                        checkpoint(checkpointer);
                    }
                }
            }
        } catch (InterruptedException ie) {
            // We failed to shutdown cleanly, do not checkpoint.
            scheduledExecutor.shutdownNow();
            // Handle this similar to a host or process crashing and abort the JVM.
            LOG.fatal("Couldn't successfully persist data within the max wait time. Aborting the JVM to mimic a crash.");
            System.exit(1);
        }
    }

    /**
     * Set the timer for the next checkpoint.
     */
    private void resetCheckpointAlarm() {
        checkpointTimer.alarmIn(config.getCheckpointIntervalInSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < config.getCheckpointRetries(); i++) {
            try {
                // First checkpoint our persister to guarantee all calculated counts have been persisted
                persister.checkpoint();
                checkpointer.checkpoint();
                return;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                return;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (config.getCheckpointRetries() - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + config.getCheckpointRetries(),
                            e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            } catch (InterruptedException e) {
                LOG.error("Error encountered while checkpointing count persister.", e);
                // Fall through to attempt retry
            }
            try {
                Thread.sleep(config.getCheckpointBackoffTimeInSeconds());
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
        // Handle this similar to a host or process crashing and abort the JVM.
        LOG.fatal("Couldn't successfully persist data within max retry limit. Aborting the JVM to mimic a crash.");
        System.exit(1);
    }
}
