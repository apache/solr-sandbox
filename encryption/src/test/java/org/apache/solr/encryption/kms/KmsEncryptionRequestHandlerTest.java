package org.apache.solr.encryption.kms;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.encryption.EncryptionTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.encryption.EncryptionRequestHandler.PARAM_KEY_ID;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_ENCRYPTION_KEY_BLOB;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_TENANT_ID;

/**
 * Tests {@link KmsEncryptionRequestHandler}.
 */
public class KmsEncryptionRequestHandlerTest extends SolrCloudTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    EncryptionTestUtil.setInstallDirProperty();
    cluster = new MiniSolrCloudCluster.Builder(2, createTempDir())
            .addConfig("config", EncryptionTestUtil.getConfigPath("collection1"))
            .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.shutdown();
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
}