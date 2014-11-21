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

import com.amazonaws.services.kinesis.samples.datavis.kcl.CountingRecordProcessor;

import java.util.Map;

/**
 * A class that is capable of persisting the counts of objects as produced by {@link CountingRecordProcessor}.
 *
 * @param <T> Type of objects this persister can persist.
 */
public interface CountPersister<T> {

    /**
     * Initialize this persister.
     */
    public void initialize();

    /**
     * Persist the map of objects to counts.
     *
     * @param objectCounts
     */
    public void persist(Map<T, Long> objectCounts);

    /**
     * Indicates this persister should flush its internal state and guarantee all records received from calls to
     * {@link #persist(Map)} are completely handled.
     *
     * @throws InterruptedException if any thread interrupted the current thread while performing a checkpoint.
     */
    public void checkpoint() throws InterruptedException;
}
