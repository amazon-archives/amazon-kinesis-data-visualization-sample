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

package com.amazonaws.services.kinesis.samples.datavis.kcl;

/**
 * The configuration settings for a {@link CountingRecordProcessor}.
 */
public class CountingRecordProcessorConfig {
    // How often to checkpoint
    private long checkpointIntervalInSeconds = 60L;
    // Backoff and retry settings for checkpointing
    private long checkpointBackoffTimeInSeconds = 3L;
    private long checkpointRetries = 10;
    // The initial amount of time to wait after initialize() is called before advancing the interval window.
    private long initialWindowAdvanceDelayInSeconds = 10L;

    public long getCheckpointIntervalInSeconds() {
        return checkpointIntervalInSeconds;
    }

    public void setCheckpointIntervalInSeconds(long checkpointIntervalInSeconds) {
        this.checkpointIntervalInSeconds = checkpointIntervalInSeconds;
    }

    public long getCheckpointBackoffTimeInSeconds() {
        return checkpointBackoffTimeInSeconds;
    }

    public void setCheckpointBackoffTimeInSeconds(long checkpointBackoffTimeInSeconds) {
        this.checkpointBackoffTimeInSeconds = checkpointBackoffTimeInSeconds;
    }

    public long getCheckpointRetries() {
        return checkpointRetries;
    }

    public void setCheckpointRetries(long checkpointRetries) {
        this.checkpointRetries = checkpointRetries;
    }

    public long getInitialWindowAdvanceDelayInSeconds() {
        return initialWindowAdvanceDelayInSeconds;
    }

    public void setInitialWindowAdvanceDelayInSeconds(long initialWindowAdvanceDelayInSeconds) {
        this.initialWindowAdvanceDelayInSeconds = initialWindowAdvanceDelayInSeconds;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (checkpointBackoffTimeInSeconds ^ (checkpointBackoffTimeInSeconds >>> 32));
        result = prime * result + (int) (checkpointIntervalInSeconds ^ (checkpointIntervalInSeconds >>> 32));
        result = prime * result + (int) (checkpointRetries ^ (checkpointRetries >>> 32));
        result =
                prime * result
                        + (int) (initialWindowAdvanceDelayInSeconds ^ (initialWindowAdvanceDelayInSeconds >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CountingRecordProcessorConfig other = (CountingRecordProcessorConfig) obj;
        if (checkpointBackoffTimeInSeconds != other.checkpointBackoffTimeInSeconds) {
            return false;
        }
        if (checkpointIntervalInSeconds != other.checkpointIntervalInSeconds) {
            return false;
        }
        if (checkpointRetries != other.checkpointRetries) {
            return false;
        }
        if (initialWindowAdvanceDelayInSeconds != other.initialWindowAdvanceDelayInSeconds) {
            return false;
        }
        return true;
    }

}
