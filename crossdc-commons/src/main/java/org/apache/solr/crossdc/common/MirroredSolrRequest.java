/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crossdc.common;

import org.apache.solr.client.solrj.SolrRequest;

import java.util.*;

/**
 * Class to encapsulate a mirrored Solr request.
 * This adds a timestamp and #attempts to the request for tracking purpopse.
 */
public class MirroredSolrRequest {
    private final SolrRequest solrRequest;

    // Attempts counter for processing the request
    private int attempt = 1;

    // Timestamp to track when this request was first written. This should be used to track the replication lag.
    private long submitTimeNanos = 0;

    public MirroredSolrRequest(final SolrRequest solrRequest) {
        this(1, solrRequest, 0);
    }

    public MirroredSolrRequest(final int attempt, final SolrRequest solrRequest, final long submitTimeNanos) {
        if (solrRequest == null) {
            throw new NullPointerException("solrRequest cannot be null");
        }
        this.attempt = attempt;
        this.solrRequest = solrRequest;
        this.submitTimeNanos = submitTimeNanos;
    }

    public MirroredSolrRequest(final int attempt,
                               final long submitTimeNanos) {
        this.attempt = attempt;
        this.submitTimeNanos = submitTimeNanos;
        solrRequest = null;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(final int attempt) {
        this.attempt = attempt;
    }

    public SolrRequest getSolrRequest() {
        return solrRequest;
    }

    public long getSubmitTimeNanos() {
        return submitTimeNanos;
    }

    public void setSubmitTimeNanos(final long submitTimeNanos) {
        this.submitTimeNanos = submitTimeNanos;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof MirroredSolrRequest)) return false;

        final MirroredSolrRequest that = (MirroredSolrRequest)o;

        return Objects.equals(solrRequest, that.solrRequest);
    }

    @Override
    public int hashCode() {
        return solrRequest.hashCode();
    }

    @Override
    public String toString() {
        return "MirroredSolrRequest{" +
               "solrRequest=" + solrRequest +
               ", attempt=" + attempt +
               ", submitTimeNanos=" + submitTimeNanos +
               '}';
    }
}
