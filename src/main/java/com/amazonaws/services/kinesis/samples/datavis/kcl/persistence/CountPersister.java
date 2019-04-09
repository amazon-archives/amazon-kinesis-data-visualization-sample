/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
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
