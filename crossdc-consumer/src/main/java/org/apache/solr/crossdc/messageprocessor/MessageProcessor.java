package org.apache.solr.crossdc.messageprocessor;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.crossdc.ResubmitBackoffPolicy;

public abstract class MessageProcessor {

  private final ResubmitBackoffPolicy resubmitBackoffPolicy;

  public MessageProcessor(ResubmitBackoffPolicy resubmitBackoffPolicy) {
    this.resubmitBackoffPolicy = resubmitBackoffPolicy;
  }

  public ResubmitBackoffPolicy getResubmitBackoffPolicy() {
    return resubmitBackoffPolicy;
  }
}
