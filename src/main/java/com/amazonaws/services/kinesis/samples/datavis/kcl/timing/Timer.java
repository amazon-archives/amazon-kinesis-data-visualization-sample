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

package com.amazonaws.services.kinesis.samples.datavis.kcl.timing;

import java.util.concurrent.TimeUnit;

/**
 * A simple timer that will indicate when an given time has been exceeded by using the provided clock.
 */
public class Timer {
    private long alarmAt = Long.MAX_VALUE;

    private Clock clock;

    public Timer(Clock clock) {
        if (clock == null) {
            throw new NullPointerException("clock must not be null");
        }
        this.clock = clock;
    }

    /**
     * Set the time to alarm at next.
     * <p/>
     * Note: that providing time units that do not match the underlying clock's time units may result in
     * truncation or overflow. See {@link TimeUnit#convert(long, java.util.concurrent.TimeUnit)} for more details.
     *
     * @param time Time value.
     * @param unit Units of the time value.
     */
    public void alarmIn(long time, TimeUnit unit) {
        // Convert the incoming time to clock time and add it to the current clock time.
        alarmAt = clock.getTime() + clock.getTimeUnit().convert(time, unit);
    }

    /**
     * Check if the time last set by {@link #alarmIn(long, java.util.concurrent.TimeUnit)}} has elapsed.
     *
     * @return {@code true} if the timer is in alarm.
     */
    public boolean isTimeUp() {
        return clock.getTime() >= alarmAt;
    }
}
