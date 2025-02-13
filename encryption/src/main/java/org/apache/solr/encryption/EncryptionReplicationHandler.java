package org.apache.solr.encryption;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.admin.api.CoreReplication;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Extension of {@link ReplicationHandler} that unwraps each {@link Directory} used to copy index file.
 * This way the encrypted files are not decrypted during the copy.
 */
public class EncryptionReplicationHandler extends ReplicationHandler {

  @Override
  protected CoreReplication createCoreReplication(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
    return new EncryptionCoreReplication(solrCore, req, rsp);
  }

  @Override
  protected Class<? extends CoreReplication> getCoreReplicationClass() {
    return EncryptionCoreReplication.class;
  }

  private static class EncryptionCoreReplication extends CoreReplication {

    EncryptionCoreReplication(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
      super(solrCore, req, rsp);
    }

    @Override
    protected Directory filterDirectory(Directory directory) {
      return FilterDirectory.unwrap(directory);
    }
  }
}
