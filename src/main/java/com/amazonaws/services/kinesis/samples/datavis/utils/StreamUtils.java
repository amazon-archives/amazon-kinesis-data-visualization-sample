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

package com.amazonaws.services.kinesis.samples.datavis.utils;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;

/**
 * A collection of functions to manipulate Amazon Kinesis streams.
 */
public class StreamUtils {
    private static final Log LOG = LogFactory.getLog(StreamUtils.class);

    private AmazonKinesis kinesis;
    private static final long CREATION_WAIT_TIME_IN_SECONDS = TimeUnit.SECONDS.toMillis(30);
    private static final long DELAY_BETWEEN_STATUS_CHECKS_IN_SECONDS = TimeUnit.SECONDS.toMillis(30);

    /**
     * Creates a new utility instance.
     *
     * @param kinesis The Amazon Kinesis client to use for all operations.
     */
    public StreamUtils(AmazonKinesis kinesis) {
        if (kinesis == null) {
            throw new NullPointerException("Amazon Kinesis client must not be null");
        }
        this.kinesis = kinesis;
    }

    /**
     * Create a stream if it doesn't already exist.
     *
     * @param streamName Name of stream
     * @param shards Number of shards to create stream with. This is ignored if the stream already exists.
     * @throws AmazonServiceException Error communicating with Amazon Kinesis.
     */
    public void createStreamIfNotExists(String streamName, int shards) throws AmazonClientException {
        try {
            if (isActive(kinesis.describeStream(streamName))) {
                return;
            }
        } catch (ResourceNotFoundException ex) {
            LOG.info(String.format("Creating stream %s...", streamName));
            // No stream, create
            kinesis.createStream(streamName, shards);
            // Initially wait a number of seconds before checking for completion
            try {
                Thread.sleep(CREATION_WAIT_TIME_IN_SECONDS);
            } catch (InterruptedException e) {
                LOG.warn(String.format("Interrupted while waiting for %s stream to become active. Aborting.", streamName));
                Thread.currentThread().interrupt();
                return;
            }
        }

        // Wait for stream to become active
        int maxRetries = 3;
        int i = 0;
        while (i < maxRetries) {
            i++;
            try {
                if (isActive(kinesis.describeStream(streamName))) {
                    return;
                }
            } catch (ResourceNotFoundException ignore) {
                // The stream may be reported as not found if it was just created.
            }
            try {
                Thread.sleep(DELAY_BETWEEN_STATUS_CHECKS_IN_SECONDS);
            } catch (InterruptedException e) {
                LOG.warn(String.format("Interrupted while waiting for %s stream to become active. Aborting.", streamName));
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new RuntimeException("Stream " + streamName + " did not become active within 2 minutes.");
    }

    /**
     * Does the result of a describe stream request indicate the stream is ACTIVE?
     *
     * @param r The describe stream result to check for ACTIVE status.
     */
    private boolean isActive(DescribeStreamResult r) {
        return "ACTIVE".equals(r.getStreamDescription().getStreamStatus());
    }

    /**
     * Delete an Amazon Kinesis stream.
     *
     * @param streamName The name of the stream to delete.
     */
    public void deleteStream(String streamName) {
        LOG.info(String.format("Deleting Kinesis stream %s", streamName));
        try {
            kinesis.deleteStream(streamName);
        } catch (ResourceNotFoundException ex) {
            // The stream could not be found.
        } catch (AmazonClientException ex) {
            LOG.error(String.format("Error deleting stream %s", streamName), ex);
        }
    }

    /**
     * Split a shard by dividing the hash key space in half.
     *
     * @param streamName Name of the stream that contains the shard to split.
     * @param shardId The id of the shard to split.
     *
     * @throws IllegalArgumentException When either streamName or shardId are null or empty.
     * @throws LimitExceededException Shard limit for the account has been reached.
     * @throws ResourceNotFoundException The stream or shard cannot be found.
     * @throws InvalidArgumentException If the shard is closed and no eligible for splitting.
     * @throws AmazonClientException Error communicating with Amazon Kinesis.
     *
     */
    public void splitShardEvenly(String streamName, String shardId)
        throws LimitExceededException, ResourceNotFoundException, AmazonClientException, InvalidArgumentException,
        IllegalArgumentException {
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("stream name is required");
        }
        if (shardId == null || shardId.isEmpty()) {
            throw new IllegalArgumentException("shard id is required");
        }

        DescribeStreamResult result = kinesis.describeStream(streamName);
        StreamDescription description = result.getStreamDescription();

        // Find the shard we want to split
        Shard shardToSplit = null;
        for (Shard shard : description.getShards()) {
            if (shardId.equals(shard.getShardId())) {
                shardToSplit = shard;
                break;
            }
        }

        if (shardToSplit == null) {
            throw new ResourceNotFoundException("Could not find shard with id '" + shardId + "' in stream '"
                    + streamName + "'");
        }

        // Check if the shard is still open. Open shards do not have an ending sequence number.
        if (shardToSplit.getSequenceNumberRange().getEndingSequenceNumber() != null) {
            throw new InvalidArgumentException("Shard is CLOSED and is not eligible for splitting");
        }

        // Calculate the median hash key to use as the new starting hash key for the shard.
        BigInteger startingHashKey = new BigInteger(shardToSplit.getHashKeyRange().getStartingHashKey());
        BigInteger endingHashKey = new BigInteger(shardToSplit.getHashKeyRange().getEndingHashKey());
        BigInteger[] medianHashKey = startingHashKey.add(endingHashKey).divideAndRemainder(new BigInteger("2"));
        BigInteger newStartingHashKey = medianHashKey[0];
        if (!BigInteger.ZERO.equals(medianHashKey[1])) {
            // In order to more evenly distributed the new hash key ranges across the new shards we will "round up" to
            // the next integer when our current hash key range is not evenly divisible by 2.
            newStartingHashKey = newStartingHashKey.add(BigInteger.ONE);
        }

        // Submit the split shard request
        kinesis.splitShard(streamName, shardId, newStartingHashKey.toString());
    }
}
