package org.apache.solr.crossdc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.consumer.Consumer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.DEFAULT_MAX_REQUEST_SIZE;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.INDEX_UNMIRRORABLE_DOCS;

@ThreadLeakFilters(defaultFilters = true, filters = { SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class })
@ThreadLeakLingering(linger = 5000) public class SolrAndKafkaMultiCollectionIntegrationTest extends
    SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_DOC_SIZE_BYTES = Integer.parseInt(DEFAULT_MAX_REQUEST_SIZE);

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public EmbeddedKafkaCluster kafkaCluster;

  protected volatile MiniSolrCloudCluster solrCluster1;
  protected volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer;

  private static String TOPIC = "topic1";

  private static String COLLECTION = "collection1";
  private static String ALT_COLLECTION = "collection2";

  @Before
  public void beforeSolrAndKafkaIntegrationTest() throws Exception {
    consumer = new Consumer();
    Properties config = new Properties();
    //config.put("unclean.leader.election.enable", "true");
    //config.put("enable.partition.eof", "false");

    kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS, config) {
      public String bootstrapServers() {
        return super.bootstrapServers().replaceAll("localhost", "127.0.0.1");
      }
    };
    kafkaCluster.start();

    kafkaCluster.createTopic(TOPIC, 1, 1);

    System.setProperty("topicName", TOPIC);
    System.setProperty("bootstrapServers", kafkaCluster.bootstrapServers());
    System.setProperty(INDEX_UNMIRRORABLE_DOCS, "false");

    solrCluster1 = new SolrCloudTestCase.Builder(1, createTempDir()).addConfig("conf",
        getFile("src/test/resources/configs/cloud-minimal/conf").toPath()).configure();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(COLLECTION, 1, 1);

    solrCluster1.getSolrClient().setDefaultCollection(COLLECTION);

    solrCluster2 = new SolrCloudTestCase.Builder(1, createTempDir()).addConfig("conf",
        getFile("src/test/resources/configs/cloud-minimal/conf").toPath()).configure();

    CollectionAdminRequest.Create create2 =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster2.getSolrClient().request(create2);
    solrCluster2.waitForActiveCollection(COLLECTION, 1, 1);

    solrCluster2.getSolrClient().setDefaultCollection(COLLECTION);

    String bootstrapServers = kafkaCluster.bootstrapServers();
    log.info("bootstrapServers={}", bootstrapServers);

    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, bootstrapServers);
    properties.put(KafkaCrossDcConf.ZK_CONNECT_STRING, solrCluster2.getZkServer().getZkAddress());
    properties.put(KafkaCrossDcConf.TOPIC_NAME, TOPIC);
    properties.put(KafkaCrossDcConf.GROUP_ID, "group1");
    properties.put(KafkaCrossDcConf.MAX_REQUEST_SIZE_BYTES, MAX_DOC_SIZE_BYTES);
    consumer.start(properties);

  }

  @After
  public void afterSolrAndKafkaIntegrationTest() throws Exception {
    ObjectReleaseTracker.clear();

    if (solrCluster1 != null) {
      solrCluster1.getZkServer().getZkClient().printLayoutToStdOut();
      solrCluster1.shutdown();
    }
    if (solrCluster2 != null) {
      solrCluster2.getZkServer().getZkClient().printLayoutToStdOut();
      solrCluster2.shutdown();
    }

    consumer.shutdown();
    consumer = null;

    try {
      //kafkaCluster.deleteAllTopicsAndWait(10000);
      kafkaCluster.stop();
      kafkaCluster = null;
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }


  }

  private static SolrInputDocument getDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");
    return doc;
  }

  private void assertCluster2EventuallyHasDocs(String collection, String query, int expectedNumDocs) throws Exception {
    assertClusterEventuallyHasDocs(solrCluster2.getSolrClient(), collection, query, expectedNumDocs);
  }

  private void createCollection(CloudSolrClient client, CollectionAdminRequest.Create createCmd) throws Exception {
    final String stashedDefault = client.getDefaultCollection();
    try {
      //client.setDefaultCollection(null);
      client.request(createCmd);
    } finally {
      //client.setDefaultCollection(stashedDefault);
    }
  }

  @Test
  public void testFullCloudToCloudMultiCollection() throws Exception {
    CollectionAdminRequest.Create create =
            CollectionAdminRequest.createCollection(ALT_COLLECTION, "conf", 1, 1);

    try {
      solrCluster1.getSolrClient().request(create);
      solrCluster1.waitForActiveCollection(ALT_COLLECTION, 1, 1);

      solrCluster2.getSolrClient().request(create);
      solrCluster2.waitForActiveCollection(ALT_COLLECTION, 1, 1);


      CloudSolrClient client = solrCluster1.getSolrClient();

      SolrInputDocument doc1 = getDoc();
      SolrInputDocument doc2 = getDoc();
      SolrInputDocument doc3 = getDoc();
      SolrInputDocument doc4 = getDoc();
      SolrInputDocument doc5 = getDoc();
      SolrInputDocument doc6 = getDoc();
      SolrInputDocument doc7 = getDoc();

      client.add(COLLECTION, doc1);
      client.add(ALT_COLLECTION, doc2);
      client.add(COLLECTION, doc3);
      client.add(COLLECTION, doc4);
      client.add(ALT_COLLECTION, doc5);
      client.add(ALT_COLLECTION, doc6);
      client.add(COLLECTION, doc7);

      client.commit(COLLECTION);
      client.commit(ALT_COLLECTION);

      System.out.println("Sent producer record");

      assertCluster2EventuallyHasDocs(ALT_COLLECTION, "*:*", 3);
      assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 4);

    } finally {
      CollectionAdminRequest.Delete delete =
              CollectionAdminRequest.deleteCollection(ALT_COLLECTION);
      solrCluster1.getSolrClient().request(delete);
      solrCluster2.getSolrClient().request(delete);
    }
  }


  private void assertClusterEventuallyHasDocs(SolrClient client, String collection, String query, int expectedNumDocs) throws Exception {
    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 100; i++) {
      client.commit(collection);
      results = client.query(collection, new SolrQuery(query));
      if (results.getResults().getNumFound() == expectedNumDocs) {
        foundUpdates = true;
      } else {
        Thread.sleep(200);
      }
    }

    assertTrue("results=" + results, foundUpdates);
  }
}
