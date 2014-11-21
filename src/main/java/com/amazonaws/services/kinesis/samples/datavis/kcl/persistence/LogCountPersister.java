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
