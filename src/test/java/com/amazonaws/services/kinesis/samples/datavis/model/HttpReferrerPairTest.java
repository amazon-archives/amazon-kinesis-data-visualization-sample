/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: MIT-0
*/

package com.amazonaws.services.kinesis.samples.datavis.model;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class HttpReferrerPairTest {

    @Test
    public void GIVEN_samePairs_WHEN_comparedForEquality_THEN_equalsAndHashCodeAreEqual() {
        HttpReferrerPair a = new HttpReferrerPair("a", "b");
        HttpReferrerPair b = new HttpReferrerPair("a", "b");

        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
        assertTrue(a.hashCode() == b.hashCode());
    }

    @Test
    public void GIVEN_differedPairs_WHEN_comparedForEquality_THEN_equalsAndHashCodeAreNotEqual() {
        HttpReferrerPair a = new HttpReferrerPair("a", "b");
        HttpReferrerPair b = new HttpReferrerPair("1", "2");

        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
        assertFalse(a.hashCode() == b.hashCode());
    }

}
