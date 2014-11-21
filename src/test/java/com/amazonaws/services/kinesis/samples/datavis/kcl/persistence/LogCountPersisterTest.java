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

package com.amazonaws.services.kinesis.samples.datavis.kcl.persistence;

import java.util.Collections;

import org.junit.Test;

public class LogCountPersisterTest {

    @Test
    public void GIVEN_persister_WHEN_persistWithCounts_THEN_noExceptionThrown() {
        LogCountPersister<String> persister = new LogCountPersister<>();
        persister.persist(Collections.singletonMap("resource-a", 100L));
    }

}
