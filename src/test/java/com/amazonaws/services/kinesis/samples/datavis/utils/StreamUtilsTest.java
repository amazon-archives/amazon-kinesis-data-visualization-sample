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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.samples.datavis.utils.StreamUtils;

public class StreamUtilsTest {

    private AmazonKinesis kinesis;
    private StreamUtils utils;

    @Before
    public void init() {
        kinesis = mock(AmazonKinesis.class);
        utils = new StreamUtils(kinesis);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_nullStreamName_WHEN_splitShardEvenly_THEN_throwException() {
        utils.splitShardEvenly(null, "shardId");
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_emptyStreamName_WHEN_splitShardEvenly_THEN_throwException() {
        utils.splitShardEvenly("", "shardId");
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_nullShardId_WHEN_splitShardEvenly_THEN_throwException() {
        utils.splitShardEvenly("stream", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_emptyShardId_WHEN_splitShardEvenly_THEN_throwException() {
        utils.splitShardEvenly("stream", "");
    }

    @Test(expected = ResourceNotFoundException.class)
    public void GIVEN_streamDoesNotExist_WHEN_splitShardEvenly_THEN_throwException() {
        String streamName = "streamName";
        String shardId = "shardId";

        // Mimic the behavior of Amazon Kinesis
        when(kinesis.describeStream(streamName)).thenThrow(new ResourceNotFoundException("stream does not exist"));

        utils.splitShardEvenly(streamName, shardId);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void GIVEN_shardDoesNotExist_WHEN_splitShardEvenly_THEN_throwException() {
        String streamName = "streamName";
        String shardId = "shardId";
        createMocksForStreamWithShard(streamName, shardId, "1", "4", true);

        utils.splitShardEvenly(streamName, "unknownShardId");
    }

    @Test
    public void GIVEN_shardWithOddHashKeyRange_WHEN_splitShardEvenly_THEN_streamContainsTwoShards() {
        String streamName = "streamName";
        String shardId = "shardId";
        createMocksForStreamWithShard(streamName, shardId, "1", "4", true);

        utils.splitShardEvenly(streamName, shardId);

        verify(kinesis).splitShard(streamName, shardId, "3");
    }

    @Test
    public void GIVEN_shardWithEvenHashKeyRange_WHEN_splitShardEvenly_THEN_streamContainsTwoShards() {
        String streamName = "streamName";
        String shardId = "shardId";
        createMocksForStreamWithShard(streamName, shardId, "1", "3", true);

        utils.splitShardEvenly(streamName, shardId);

        verify(kinesis).splitShard(streamName, shardId, "2");
    }

    @Test(expected = InvalidArgumentException.class)
    public void GIVEN_closedShard_WHEN_splitShardEvenly_THEN_throwException() {
        String streamName = "streamName";
        String shardId = "shardId";
        createMocksForStreamWithShard(streamName, shardId, "1", "3", false);

        utils.splitShardEvenly(streamName, shardId);
    }

    /**
     * Create the necessary mocked objects to represent a valid DescribeStream request for a given stream and shard with
     * the provided hash keys. It's possible to create a request mock that represents a closed shard that is ineligible
     * for splitting by providing {@code isOpen = false}.
     *
     * @param streamName The name of the stream.
     * @param shardId The name of the shard.
     * @param startingHashKey The starting hash key of the shard.
     * @param endingHashKey The ending hash key of the shard.
     * @param isOpen Is the shard open? If not, an ending sequence number will be provided in the
     *        {@link SequenceNumberRange}.
     */
    private void createMocksForStreamWithShard(String streamName,
            String shardId,
            String startingHashKey,
            String endingHashKey,
            boolean isOpen) {

        // Create the shard as requested
        Shard shard = new Shard();
        shard.setShardId(shardId);
        // Create the appropriate hash key range
        HashKeyRange hashKeyRange = new HashKeyRange();
        hashKeyRange.setStartingHashKey(startingHashKey);
        hashKeyRange.setEndingHashKey(endingHashKey);
        shard.setHashKeyRange(hashKeyRange);
        // Create a fictitious sequence number range to represent the state of the shard.
        SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
        sequenceNumberRange.setStartingSequenceNumber("0");
        if (!isOpen) {
            // A closed shard has a non-null ending sequence number
            sequenceNumberRange.setEndingSequenceNumber("1");
        }
        shard.setSequenceNumberRange(sequenceNumberRange);

        // Mock the parts of the result we depend upon for our tests
        DescribeStreamResult describeStreamResult = mock(DescribeStreamResult.class);
        StreamDescription description = mock(StreamDescription.class);
        when(kinesis.describeStream(streamName)).thenReturn(describeStreamResult);
        when(describeStreamResult.getStreamDescription()).thenReturn(description);
        when(description.getShards()).thenReturn(Collections.singletonList(shard));
    }
}
