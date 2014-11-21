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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Permission;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samples.datavis.kcl.persistence.CountPersister;
import com.amazonaws.services.kinesis.samples.datavis.model.HttpReferrerPair;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CountingRecordProcessorTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private CountPersister<HttpReferrerPair> persister;
    private IRecordProcessorCheckpointer checkpointer;
    private CountingRecordProcessor<HttpReferrerPair> processor;
    private CountingRecordProcessorConfig config;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        persister = mock(CountPersister.class);
        checkpointer = mock(IRecordProcessorCheckpointer.class);

        config = new CountingRecordProcessorConfig();
        config.setCheckpointBackoffTimeInSeconds(0L);
        config.setCheckpointIntervalInSeconds(0L);
        config.setInitialWindowAdvanceDelayInSeconds(1L);

        // Create a processor that will calculate 10 intervals every second
        processor = new CountingRecordProcessor<>(config, HttpReferrerPair.class, persister, 1000, 100);
        processor.initialize("shardId");
    }

    @After
    public void tearDown() {
        processor.shutdown(null, ShutdownReason.ZOMBIE);
    }

    @Test(expected = NullPointerException.class)
    public void GIVEN_processor_WHEN_nullConfigProvided_THEN_throwException() {
        new CountingRecordProcessor<>(null, HttpReferrerPair.class, persister, 100, 10);
    }

    @Test(expected = NullPointerException.class)
    public void GIVEN_processor_WHEN_nullRecordTypeProvided_THEN_throwException() {
        new CountingRecordProcessor<>(config, null, persister, 100, 10);
    }

    @Test(expected = NullPointerException.class)
    public void GIVEN_processor_WHEN_nullPersisterProvided_THEN_throwException() {
        new CountingRecordProcessor<>(config, HttpReferrerPair.class, null, 100, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_processor_WHEN_rangeIsZero_THEN_throwException() {
        new CountingRecordProcessor<>(config, HttpReferrerPair.class, persister, 0, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_processor_WHEN_intervalIsZero_THEN_throwException() {
        new CountingRecordProcessor<>(config, HttpReferrerPair.class, persister, 10, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_processor_WHEN_rangeIsNotEvenlyDivisibleByIntervalLength_THEN_throwException() {
        new CountingRecordProcessor<>(config, HttpReferrerPair.class, persister, 100, 80);
    }

    @Test(timeout = 3000)
    public void GIVEN_processor_WHEN_lessThanRangeTimeHasElapsed_THEN_processorNeverCalledPersist()
        throws InterruptedException {
        long start = System.nanoTime();
        // Sleep for less than two intervals after our initial delay. This is not enough time to fill the window.
        Thread.sleep(TimeUnit.SECONDS.toMillis(config.getInitialWindowAdvanceDelayInSeconds()) + 900);
        long end = System.nanoTime();

        // Make sure we slept for less than 1 full window.
        assertTrue((end - start) < TimeUnit.SECONDS.toNanos(config.getInitialWindowAdvanceDelayInSeconds() + 1));

        // Make sure the persister is not called since we don't haven't had time to collect a full window of data
        verify(persister, never()).persist(anyMapOf(HttpReferrerPair.class, Long.class));
    }

    @Test(timeout = 5000)
    public void GIVEN_processor_WHEN_moreThanRangeTimeHasElapsed_THEN_processorCallsPersist()
        throws InterruptedException {
        long start = System.nanoTime();
        // Sleep for slightly more than 1 full window after the initial delay of 5 seconds. This is enough to fill the
        // window.
        Thread.sleep(TimeUnit.SECONDS.toMillis(config.getInitialWindowAdvanceDelayInSeconds()) + 1100);
        long end = System.nanoTime();

        // Make sure we slept for at least 1 second to guarantee we have a full window
        assertTrue((end - start) >= TimeUnit.SECONDS.toNanos(config.getInitialWindowAdvanceDelayInSeconds() + 1));

        // Make sure our persister was called at least once
        verify(persister, atLeast(1)).persist(anyMapOf(HttpReferrerPair.class, Long.class));
    }

    private Record createRecordFrom(HttpReferrerPair pair) {
        Record record = new Record();
        record.setPartitionKey(pair.getResource());
        record.setSequenceNumber("dummy");
        try {
            record.setData(ByteBuffer.wrap(JSON.writeValueAsBytes(pair)));
        } catch (IOException e) {
            throw new RuntimeException("Error generating JSON from pair count: " + pair, e);
        }
        return record;
    }

    @Test
    public void GIVEN_recordsHaveBeenProcessed_WHEN_advanceOneInterval_THEN_persisterIsCalledWithNewCounts() {
        HttpReferrerPair pair = new HttpReferrerPair();
        pair.setResource("a");
        pair.setReferrer("b");

        // Advance enough intervals to fill the window
        for (int i = 0; i < 10; i++) {
            processor.advanceOneInterval();
        }

        // Process a single pair
        processor.processRecords(Arrays.asList(createRecordFrom(pair)), checkpointer);

        // When we advance to the next interval our persister should be called with a single count for our pair.
        Map<HttpReferrerPair, Long> expectedCounts = Collections.singletonMap(pair, 1L);

        processor.advanceOneInterval();

        verify(persister).persist(expectedCounts);
    }

    @Test
    public void GIVEN_existingCounts_WHEN_malformedRecordReceived_THEN_badRecordIsSkippedAndCountsRemainCorrect() {
        HttpReferrerPair pair = new HttpReferrerPair();
        pair.setResource("a");
        pair.setReferrer("b");

        // Advance enough intervals to fill the window
        for (int i = 0; i < 10; i++) {
            processor.advanceOneInterval();
        }

        // Process a single pair
        processor.processRecords(Collections.singletonList(createRecordFrom(pair)), checkpointer);

        Record badRecord = createRecordFrom(pair);
        badRecord.setData(ByteBuffer.wrap(new byte[1]));
        processor.processRecords(Collections.singletonList(badRecord), checkpointer);

        processor.advanceOneInterval();

        // We should have a single count for the good record
        Map<HttpReferrerPair, Long> expectedCounts = Collections.singletonMap(pair, 1L);

        verify(persister).persist(expectedCounts);
    }

    @Test
    public void GIVEN_runningProcessor_WHEN_shutdownAsTerminated_THEN_checkpointerInvoked() throws Exception {
        processor.shutdown(checkpointer, ShutdownReason.TERMINATE);

        verify(checkpointer).checkpoint();
    }

    @Test
    public void GIVEN_itIsTimeToCheckpoint_WHEN_processRecords_THEN_checkpointIsPerformed() throws Exception {
        processor.processRecords(Collections.<Record> emptyList(), checkpointer);

        verify(checkpointer).checkpoint();
    }

    @Test
    public void GIVEN_DynamoDBIsThrottlingOurWritesToKCLLeaseTable_WHEN_checkpoint_THEN_retry() throws Exception {
        // Throw an exception the first invocation, do nothing (complete successfully) the second invocation
        doThrow(new ThrottlingException("test")).doNothing().when(checkpointer).checkpoint();

        processor.shutdown(checkpointer, ShutdownReason.TERMINATE);

        // checkpoint should be retried a second time
        verify(checkpointer, times(2)).checkpoint();
    }

    @Test(timeout = 1000)
    public void
            GIVEN_DynamoDBIsThrottlingOurWritesTOKCLLeaseTable_WHEN_checkpointFailsIndefinitely_THEN_stopTryingAfterMaxRetries()
                throws Exception {
        expectSystemExitWhenExecuting(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                // Throw a throttling exception every time we attempt to checkpoint
                doThrow(new ThrottlingException("test")).when(checkpointer).checkpoint();

                processor.shutdown(checkpointer, ShutdownReason.TERMINATE);

                verify(checkpointer, times(10)).checkpoint();

                return null;
            }
        });
    }

    @Test
    public void GIVEN_shuttingDown_WHEN_checkpointFailsWithShutdownException_THEN_checkpointSkipped() throws Exception {
        doThrow(new ShutdownException("test")).when(checkpointer).checkpoint();

        processor.shutdown(checkpointer, ShutdownReason.TERMINATE);

        verify(checkpointer).checkpoint();
    }

    @Test
    public void GIVEN_shuttingDown_WHEN_checkpointFailsWithInvalidStateException_THEN_checkpointSkipped()
        throws Exception {
        expectSystemExitWhenExecuting(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                doThrow(new InvalidStateException("test")).when(checkpointer).checkpoint();

                processor.shutdown(checkpointer, ShutdownReason.TERMINATE);

                verify(checkpointer).checkpoint();

                return null;
            }
        });
    }

    /**
     * A test helper to prevent calls to System.exit() from existing our JVM. We need to test failure behavior and want
     * to know if System.exit() was called.
     *
     * @param testBlock A code block that is expected to call System.exit().
     */
    private void expectSystemExitWhenExecuting(Callable<Void> testBlock) throws Exception {
        final SecurityException expectedPreventionOfSystemExit =
                new SecurityException("System.exit not allowed for this test.");
        // Disable System.exit() for this test
        final SecurityManager sm = new SecurityManager() {
            @Override
            public void checkExit(int status) {
                throw expectedPreventionOfSystemExit;
            }

            @Override
            public void checkPermission(Permission perm) {
                // Do nothing, allowing this security manager to be replaced
            }
        };
        SecurityManager oldSm = System.getSecurityManager();
        System.setSecurityManager(sm);
        boolean systemExitCalled = false;
        try {
            testBlock.call();
            fail("Expected System.exit to be called and throw a SecurityException by our test SecurityManager");
        } catch (SecurityException ex) {
            assertEquals("Expected SecurityException to be thrown when System.exit called",
                    expectedPreventionOfSystemExit,
                    ex);
            systemExitCalled = true;
        } finally {
            System.setSecurityManager(oldSm);
        }
        assertTrue("Expected test to call System.exit", systemExitCalled);
    }

    @Test
    public void GIVEN_recordNotReceivedForEntireRange_WHEN_advanceOneInterval_THEN_persisterCalledWithZeroCounts() {
        HttpReferrerPair pair = new HttpReferrerPair();
        pair.setResource("a");
        pair.setReferrer("b");

        // Advance enough intervals to fill the window
        for (int i = 0; i < 10; i++) {
            processor.advanceOneInterval();
        }

        // Process a single pair
        processor.processRecords(Arrays.asList(createRecordFrom(pair)), checkpointer);

        // When we advance to the next interval our persister should be called with a single count for our pair.
        processor.advanceOneInterval();
        verify(persister).persist(Collections.singletonMap(pair, 1L));

        // Advance enough times to clear the window (once before and 9 times here = 10 elapsed intervals)
        for (int i = 0; i < 9; i++) {
            processor.advanceOneInterval();
        }

        // Verify we have a count of 0 for our resource on the first interval immediately after it leaves the window
        processor.advanceOneInterval();
        verify(persister).persist(Collections.singletonMap(pair, 0L));

        // Advance once more and verify the resource is no longer returned with any counts
        processor.advanceOneInterval();
        verify(persister).persist(Collections.<HttpReferrerPair, Long> emptyMap());
    }
}
