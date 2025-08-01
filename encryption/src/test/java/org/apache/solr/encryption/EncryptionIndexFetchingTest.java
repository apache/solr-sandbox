package org.apache.solr.encryption;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.encryption.EncryptionDirectoryFactory.PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;

/**
 * Tests encrypted index fetching, when follower replicas fetch index from the leader.
 */
public class EncryptionIndexFetchingTest extends SolrCloudTestCase {

  private static final String COLLECTION_PREFIX = EncryptionIndexFetchingTest.class.getSimpleName() + "-collection-";

  private String collectionName;
  private CloudSolrClient solrClient;
  private EncryptionTestUtil testUtil;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY, EncryptionRequestHandlerTest.MockFactory.class.getName());
    EncryptionTestUtil.setInstallDirProperty();
    cluster = new MiniSolrCloudCluster.Builder(2, createTempDir())
        .addConfig("config", EncryptionTestUtil.getConfigPath("kms"))
        .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.shutdown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionName = COLLECTION_PREFIX + UUID.randomUUID();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, null, 1, 1, 0, 1)
        .process(solrClient);
    cluster.waitForActiveCollection(collectionName, 1, 2);
    testUtil = new EncryptionTestUtil(solrClient, collectionName);
  }

  @Override
  public void tearDown() throws Exception {
    CollectionAdminRequest.deleteCollection(collectionName).process(solrClient);
    super.tearDown();
  }

  @Test
  public void testIndexFetchingWithPullReplica() throws Exception {
    // GIVEN a Solr Cloud cluster composed of 2 nodes, 1 shard, 1 NRT replica and 1 PULL replica.
    // GIVEN an encrypted index containing 3 docs.
    testUtil.encryptAndExpectCompletion(KEY_ID_1);
    testUtil.indexDocsAndCommit("weather broadcast", "sunny weather", "foggy weather");

    // WHEN the follower PULL replica is queried.
    Replica follower = getFollowerReplica();
    try (Http2SolrClient followerClient = new Http2SolrClient.Builder(follower.getCoreUrl()).build()) {
      new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME)
          .waitFor(null, () -> {
            try {

              // THEN eventually the PULL replica is able to fetch the encrypted index
              // and search it to find the 3 matching docs.
              QueryResponse response = followerClient.query(new SolrQuery("weather"));
              return response.getResults().size() == 3;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }

    // THEN verify that the index is encrypted on all replicas, including the follower.
    EncryptionRequestHandlerTest.forceClearText = true;
    testUtil.assertCannotReloadCores(false);
    EncryptionRequestHandlerTest.forceClearText = false;
    testUtil.reloadCores(false);
  }

  private Replica getFollowerReplica() {
    for (Slice slice : solrClient.getClusterState().getCollection(collectionName).getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        if (!replica.isLeader()) {
          return replica;
        }
      }
    }
    throw new IllegalStateException("No follower replica found");
  }
}
