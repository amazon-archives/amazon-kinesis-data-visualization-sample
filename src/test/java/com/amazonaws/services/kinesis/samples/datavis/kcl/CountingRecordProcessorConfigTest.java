/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.datavis.kcl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CountingRecordProcessorConfigTest {

    @Test
    public void GIVEN_sameConfig_WHEN_comparedForEquality_THEN_equalsAndHashCodeAreEqual() {
        CountingRecordProcessorConfig a = new CountingRecordProcessorConfig();
        CountingRecordProcessorConfig b = new CountingRecordProcessorConfig();

        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
        assertTrue(a.hashCode() == b.hashCode());
    }

    @Test
    public void GIVEN_differentConfig_WHEN_comparedForEquality_THEN_equalsAndHashCodeAreNotEqual() {
        CountingRecordProcessorConfig a = new CountingRecordProcessorConfig();
        CountingRecordProcessorConfig b = new CountingRecordProcessorConfig();
        b.setCheckpointBackoffTimeInSeconds(10);

        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
        assertFalse(a.hashCode() == b.hashCode());
    }
}
