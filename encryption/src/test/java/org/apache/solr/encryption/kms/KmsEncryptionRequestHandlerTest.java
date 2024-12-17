package org.apache.solr.encryption.kms;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.encryption.EncryptionRequestHandlerTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.solr.encryption.EncryptionRequestHandler.PARAM_KEY_ID;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_ENCRYPTION_KEY_BLOB;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_TENANT_ID;

/**
 * Tests {@link KmsEncryptionRequestHandler}.
 */
public class KmsEncryptionRequestHandlerTest extends EncryptionRequestHandlerTest {

  static {
    configDir = "kms";
  }

  @Test(expected = SolrException.class)
  public void testEncryptRequest_NoEncryptionKeyBlobParam() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(PARAM_KEY_ID, "keyId");
    params.set(PARAM_TENANT_ID, "tenantId");
    cluster.getSolrClient().request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/encrypt", params));
  }

  @Test(expected = SolrException.class)
  public void testEncryptRequest_NoTenantIdParam() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(PARAM_KEY_ID, "keyId");
    params.set(PARAM_ENCRYPTION_KEY_BLOB, "keyBlob");
    cluster.getSolrClient().request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/encrypt", params));
  }

  @Ignore
  @Override
  // Do not test timeout because the "kms" config does not define the TestingEncryptionRequestHandler
  // required for the test.
  public void testDistributionTimeout() {}

  @Ignore
  @Test
  // Do not test state because the "kms" config does not define the TestingEncryptionRequestHandler
  // required for the test.
  public void testDistributionState() {}
}