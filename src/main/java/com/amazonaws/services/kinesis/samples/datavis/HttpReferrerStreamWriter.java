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

package com.amazonaws.services.kinesis.samples.datavis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.samples.datavis.model.HttpReferrerPair;
import com.amazonaws.services.kinesis.samples.datavis.producer.HttpReferrerKinesisPutter;
import com.amazonaws.services.kinesis.samples.datavis.producer.HttpReferrerPairFactory;
import com.amazonaws.services.kinesis.samples.datavis.utils.SampleUtils;
import com.amazonaws.services.kinesis.samples.datavis.utils.StreamUtils;

/**
 * A command-line tool that sends records to Kinesis.
 */
public class HttpReferrerStreamWriter {
    private static final Log LOG = LogFactory.getLog(HttpReferrerStreamWriter.class);

    /**
     * The amount of time to wait between records.
     *
     * We want to send at most 10 records per second per thread so we'll delay 100ms between records.
     * This keeps the overall cost low for this sample.
     */
    private static final long DELAY_BETWEEN_RECORDS_IN_MILLIS = 100;

    /**
     * Start a number of threads and send randomly generated {@link HttpReferrerPair}s to a Kinesis Stream until the
     * program is terminated.
     *
     * @param args Expecting 3 arguments: A numeric value indicating the number of threads to use to send
     *        data to Kinesis and the name of the stream to send records to, and the AWS region in which these resources
     *        exist or should be created.
     * @throws InterruptedException If this application is interrupted while sending records to Kinesis.
     */
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: " + HttpReferrerStreamWriter.class.getSimpleName()
                    + " <number of threads> <stream name> <region>");
            System.exit(1);
        }

        int numberOfThreads = Integer.parseInt(args[0]);
        String streamName = args[1];
        Region region = SampleUtils.parseRegion(args[2]);

        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        ClientConfiguration clientConfig = SampleUtils.configureUserAgentForSample(new ClientConfiguration());
        AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider, clientConfig);
        kinesis.setRegion(region);

        // The more resources we declare the higher write IOPS we need on our DynamoDB table.
        // We write a record for each resource every interval.
        // If interval = 500ms, resource count = 7 we need: (1000/500 * 7) = 14 write IOPS minimum.
        List<String> resources = new ArrayList<>();
        resources.add("/index.html");

        // These are the possible referrers to use when generating pairs
        List<String> referrers = new ArrayList<>();
        referrers.add("http://www.amazon.com");
        referrers.add("http://www.google.com");
        referrers.add("http://www.yahoo.com");
        referrers.add("http://www.bing.com");
        referrers.add("http://www.stackoverflow.com");
        referrers.add("http://www.reddit.com");

        HttpReferrerPairFactory pairFactory = new HttpReferrerPairFactory(resources, referrers);

        // Creates a stream to write to with 2 shards if it doesn't exist
        StreamUtils streamUtils = new StreamUtils(kinesis);
        streamUtils.createStreamIfNotExists(streamName, 2);
        LOG.info(String.format("%s stream is ready for use", streamName));

        final HttpReferrerKinesisPutter putter = new HttpReferrerKinesisPutter(pairFactory, kinesis, streamName);

        ExecutorService es = Executors.newCachedThreadPool();

        Runnable pairSender = new Runnable() {
            @Override
            public void run() {
                try {
                    putter.sendPairsIndefinitely(DELAY_BETWEEN_RECORDS_IN_MILLIS, TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    LOG.warn("Thread encountered an error while sending records. Records will no longer be put by this thread.",
                            ex);
                }
            }
        };

        for (int i = 0; i < numberOfThreads; i++) {
            es.submit(pairSender);
        }

        LOG.info(String.format("Sending pairs with a %dms delay between records with %d thread(s).",
                DELAY_BETWEEN_RECORDS_IN_MILLIS,
                numberOfThreads));

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
