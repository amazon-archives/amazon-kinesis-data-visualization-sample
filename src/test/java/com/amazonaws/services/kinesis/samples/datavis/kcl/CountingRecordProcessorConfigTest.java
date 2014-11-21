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
