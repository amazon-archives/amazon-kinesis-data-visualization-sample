/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.datavis.kcl.timing;

import java.util.concurrent.TimeUnit;

/**
 * A clock tells time.
 */
public interface Clock {
    /**
     * @return the current time of this clock.
     */
    public long getTime();

    /**
     * @return the units {@link #getTime()} responds with.
     */
    public TimeUnit getTimeUnit();
}
