/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.datavis.model;

/**
 * A pair of HTTP resource and HTTP referrer header field that linked to the resource.
 */
public class HttpReferrerPair {
    private String resource;
    private String referrer;

    public HttpReferrerPair() {
    }

    public HttpReferrerPair(String resource, String referrer) {
        this.resource = resource;
        this.referrer = referrer;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpReferrerPair that = (HttpReferrerPair) o;

        if (referrer != null ? !referrer.equals(that.referrer) : that.referrer != null) return false;
        if (resource != null ? !resource.equals(that.resource) : that.resource != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = resource != null ? resource.hashCode() : 0;
        result = 31 * result + (referrer != null ? referrer.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HttpReferrerPair{" +
                "resource='" + resource + '\'' +
                ", referrer='" + referrer + '\'' +
                '}';
    }
}
