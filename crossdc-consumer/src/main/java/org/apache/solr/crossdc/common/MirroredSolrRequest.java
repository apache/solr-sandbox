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
import org.apache.solr.common.params.SolrParams;

import java.util.*;
import java.util.concurrent.TimeUnit;

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

    public MirroredSolrRequest() {
        this(1, 0);
    }

    public MirroredSolrRequest(final int attempt,
                               final long submitTimeNanos) {
        this.attempt = attempt;
        this.submitTimeNanos = submitTimeNanos;
        solrRequest = null;
    }

    public static MirroredSolrRequest mirroredAdminCollectionRequest(SolrParams params) {
        Map<String, List<String>> createParams = new HashMap();
        // don't mirror back
        createParams.put(CrossDcConstants.SHOULD_MIRROR, Collections.singletonList("false"));

        final Iterator<String> paramNamesIterator = params.getParameterNamesIterator();
        while (paramNamesIterator.hasNext()) {
            final String key = paramNamesIterator.next();
            if (key.equals("createNodeSet") || key.equals("node")) {
                // don't forward as nodeset most likely makes no sense here.
                continue;
            }
            final String[] values = params.getParams(key);
            if (values != null) {
                createParams.put(key, Arrays.asList(values));
            }
        }

        return new MirroredSolrRequest(1,
                TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()));
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
