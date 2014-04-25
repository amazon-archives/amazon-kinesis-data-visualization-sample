/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.datavis.utils;

import com.amazonaws.ClientConfiguration;

/**
 * A collection of utilities for the Amazon Kinesis sample application.
 */
public class SampleUtils {

    /**
     * Creates a new client configuration with a uniquely identifiable value for this sample application.
     *
     * @param clientConfig The client configuration to copy.
     * @return A new client configuration based on the provided one with its user agent overridden.
     */
    public static ClientConfiguration configureUserAgentForSample(ClientConfiguration clientConfig) {
        ClientConfiguration newConfig = new ClientConfiguration(clientConfig);
        StringBuilder userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT);

        // Separate regions of the UserAgent with a space
        userAgent.append(" ");
        // Append the repository name followed by version number of the sample
        userAgent.append("amazon-kinesis-data-visualization-sample/1.0.0");

        newConfig.setUserAgent(userAgent.toString());

        return newConfig;
    }

}
