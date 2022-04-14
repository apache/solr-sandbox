package org.apache.solr.crossdc;

import org.apache.solr.crossdc.common.MirroredSolrRequest;

public interface ResubmitBackoffPolicy {
  long getBackoffTimeMs(MirroredSolrRequest resubmitRequest);
}
