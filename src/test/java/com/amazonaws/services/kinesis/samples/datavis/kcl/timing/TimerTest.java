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

import com.amazonaws.services.kinesis.samples.datavis.kcl.timing.Clock;
import com.amazonaws.services.kinesis.samples.datavis.kcl.timing.Timer;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimerTest {

    /**
     * A clock that responds in milliseconds with whatever time it was last told to use.
     */
    private static class TestMillisClock implements Clock {
        private long now;

        public void setTime(long now) {
            this.now = now;
        }

        @Override
        public long getTime() {
            return now;
        }

        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }
    }

    private TestMillisClock clock;
    private Timer timer;

    @Before
    public void before() {
        clock = new TestMillisClock();
        timer = new Timer(clock);
    }

    @Test(expected = NullPointerException.class)
    public void GIVEN_nullClock_WHEN_constructed_THEN_throwsNPE() {
        new Timer(null);
    }

    @Test
    public void GIVEN_alarmSetWithMorePreciseTimeUnitThanClock_WHEN_isTimeUpWithoutClockTick_THEN_isTimeUpReturnsTrue() {
        timer.alarmIn(5, TimeUnit.NANOSECONDS);

        // Nanoseconds is too granular of a time unit so it effectively
        assertTrue(timer.isTimeUp());
    }

    @Test
    public void GIVEN_alarmSet_WHEN_clockNotElapsedAlarmTime_THEN_isTimeUpReturnsFalse() {
        clock.setTime(1);
        timer.alarmIn(2, clock.getTimeUnit());
        clock.setTime(2);
        assertFalse(timer.isTimeUp());
    }

    @Test
    public void GIVEN_alarmSet_WHEN_clockAtAlarmTime_THEN_isTimeUpReturnsTrue() {
        clock.setTime(1);
        timer.alarmIn(2, clock.getTimeUnit());
        clock.setTime(4);
        assertTrue(timer.isTimeUp());
    }

}
