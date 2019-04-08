/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.datavis.kcl.persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * Writes object counts out via a logger at level INFO.
 *
 * @param <T> Type of objects this persister can persist.
 */
public class LogCountPersister<T> implements CountPersister<T> {
    private static final Log LOG = LogFactory.getLog(LogCountPersister.class);

    @Override
    public void initialize() {
    }

    @Override
    public void persist(Map<T, Long> objectCounts) {
        if (!objectCounts.isEmpty()) {
            LOG.info("Current totals:");
            LOG.info("----------------------------------------");
            for (Map.Entry<T, Long> entry : objectCounts.entrySet()) {
                LOG.info(String.format("%s\t%s", entry.getKey(), entry.getValue()));
            }
            LOG.info("----------------------------------------");
        }
    }

    @Override
    public void checkpoint() {
        // Nothing to do, we don't persist counts anywhere
    }
}
