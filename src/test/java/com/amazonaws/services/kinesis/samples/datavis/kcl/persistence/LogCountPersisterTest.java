/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
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
