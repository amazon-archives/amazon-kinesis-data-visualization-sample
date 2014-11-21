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

package com.amazonaws.services.kinesis.samples.datavis.model;

import java.util.Date;
import java.util.List;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.kinesis.samples.datavis.kcl.persistence.ddb.ReferrerCountMarshaller;

/**
 * A resource with referrers and the number of occurrences they referred to the resource over a given period of time.
 */
@DynamoDBTable(tableName = "KinesisDataVisSample-NameToBeReplacedByDynamoDBMapper")
public class HttpReferrerPairsCount {
    private String resource;
    // The timestamp when the counts were calculated
    private Date timestamp;
    // Store the hostname of the worker that updated the count
    private String host;
    // Ordered list of referrer counts in descending order. Top N can be simply obtained by inspecting the first N
    // counts.
    private List<ReferrerCount> referrerCounts;

    @DynamoDBHashKey
    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    @DynamoDBRangeKey
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @DynamoDBAttribute
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @DynamoDBAttribute
    @DynamoDBMarshalling(marshallerClass = ReferrerCountMarshaller.class)
    public List<ReferrerCount> getReferrerCounts() {
        return referrerCounts;
    }

    public void setReferrerCounts(List<ReferrerCount> referrerCounts) {
        this.referrerCounts = referrerCounts;
    }
}
