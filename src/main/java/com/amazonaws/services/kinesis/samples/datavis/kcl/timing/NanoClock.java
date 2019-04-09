/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.datavis.kcl.timing;

import java.util.concurrent.TimeUnit;

/**
 * A clock that responds in nanoseconds since Jan 1, 1970. This is backed by {@link System#nanoTime()}.
 */
public class NanoClock implements Clock {
    @Override
    public long getTime() {
        return System.nanoTime();
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.NANOSECONDS;
    }
}
