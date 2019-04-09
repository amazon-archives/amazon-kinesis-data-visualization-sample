/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.datavis.kcl.persistence.ddb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.JsonMarshaller;
import com.amazonaws.services.kinesis.samples.datavis.model.ReferrerCount;

/**
 * Marshall {@link ReferrerCount}s as JSON strings when using the {@link DynamoDBMapper}.
 */
public class ReferrerCountMarshaller extends JsonMarshaller<ReferrerCount> {
}
