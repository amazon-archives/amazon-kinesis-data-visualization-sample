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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.kinesis.samples.datavis.kcl.persistence.CountPersister;

public class CountingRecordProcessorFactoryTest {

    private static final Class<Object> RECORD_TYPE = Object.class;

    private CountPersister<Object> persister;

    @SuppressWarnings("unchecked")
    @Before
    public void init() {
        persister = mock(CountPersister.class);
    }

    @Test(expected = NullPointerException.class)
    public void GIVEN_nullRecordType_WHEN_constructed_THEN_throwException() {
        new CountingRecordProcessorFactory<>(null, persister, 10, 1);
    }

    @Test(expected = NullPointerException.class)
    public void GIVEN_nullPersister_WHEN_constructed_THEN_throwException() {
        new CountingRecordProcessorFactory<>(RECORD_TYPE, null, 10, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_negativeComputeRange_WHEN_constructed_THEN_throwException() {
        new CountingRecordProcessorFactory<>(RECORD_TYPE, persister, -10, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_negativeComputeInterval_WHEN_constructed_THEN_throwException() {
        new CountingRecordProcessorFactory<>(RECORD_TYPE, persister, 10, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GIVEN_computeRangeNotEvenlyDivisibleByInterval_WHEN_constructed_THEN_throwException() {
        new CountingRecordProcessorFactory<>(RECORD_TYPE, persister, 10, 3);
    }

    @Test
    public void GIVEN_factory_WHEN_createProcessor_THEN_processorCreated() {
        CountingRecordProcessorFactory<Object> factory =
                new CountingRecordProcessorFactory<>(RECORD_TYPE, persister, 10, 1);

        assertNotNull(factory.createProcessor());
    }
}
