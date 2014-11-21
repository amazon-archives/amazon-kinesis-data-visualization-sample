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

package com.amazonaws.services.kinesis.samples.datavis.producer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.services.kinesis.samples.datavis.model.HttpReferrerPair;

/**
 * Generates random {@link HttpReferrerPair}s based on an internal sample set. This class is thread safe.
 */
public class HttpReferrerPairFactory {
    private List<String> resources;
    private List<String> referrers;

    /**
     * Create a new generator which will use the resources and referrers provided.
     *
     * @param resources List of resources to use when generating a pair.
     * @param referrers List of referrers to use when generating a pair.
     */
    public HttpReferrerPairFactory(List<String> resources, List<String> referrers) {
        if (resources == null || resources.isEmpty()) {
            throw new IllegalArgumentException("At least 1 resource is required");
        }
        if (referrers == null || referrers.isEmpty()) {
            throw new IllegalArgumentException("At least 1 referrer is required");
        }
        this.resources = resources;
        this.referrers = referrers;
    }

    /**
     * Creates a new referrer pair using random resources and referrers from the collections provided when this
     * factory was created.
     *
     * @return A new pair with random resource and referrer values.
     */
    public HttpReferrerPair create() {
        String resource = getRandomResource();
        String referrer = getRandomReferrer();

        HttpReferrerPair pair = new HttpReferrerPair(resource, referrer);

        return pair;
    }

    /**
     * Gets a random resource from the collection of resources.
     *
     * @return A random resource.
     */
    protected String getRandomResource() {
        return resources.get(ThreadLocalRandom.current().nextInt(resources.size()));
    }

    /**
     * Gets a random referrer from the collection of referrers.
     *
     * @return A random referrer.
     */
    protected String getRandomReferrer() {
        return referrers.get(ThreadLocalRandom.current().nextInt(referrers.size()));
    }

}
