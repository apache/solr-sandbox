package org.apache.solr.crossdc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
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
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

@ThreadLeakFilters(defaultFilters = true, filters = { SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class })
@ThreadLeakLingering(linger = 5000) public class SolrAndKafkaReindexTest extends
    SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
  protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer = new Consumer();

  private static String TOPIC = "topic1";

  private static String COLLECTION = "collection1";

  @BeforeClass
  public static void beforeSolrAndKafkaIntegrationTest() throws Exception {

    Properties config = new Properties();
    config.put("unclean.leader.election.enable", "true");
    config.put("enable.partition.eof", "false");

    kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS, config) {
      public String bootstrapServers() {
        return super.bootstrapServers().replaceAll("localhost", "127.0.0.1");
      }
    };
    kafkaCluster.start();

    kafkaCluster.createTopic(TOPIC, 1, 1);

    System.setProperty("topicName", TOPIC);
    System.setProperty("bootstrapServers", kafkaCluster.bootstrapServers());

    solrCluster1 = new SolrCloudTestCase.Builder(3, createTempDir()).addConfig("conf",
        getFile("src/test/resources/configs/cloud-minimal/conf").toPath()).configure();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 2).setMaxShardsPerNode(10);;
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(COLLECTION, 3, 6);

    solrCluster1.getSolrClient().setDefaultCollection(COLLECTION);

    solrCluster2 = new SolrCloudTestCase.Builder(3, createTempDir()).addConfig("conf",
        getFile("src/test/resources/configs/cloud-minimal/conf").toPath()).configure();

    CollectionAdminRequest.Create create2 =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 3).setMaxShardsPerNode(10);
    solrCluster2.getSolrClient().request(create2);
    solrCluster2.waitForActiveCollection(COLLECTION, 2, 6);

    solrCluster2.getSolrClient().setDefaultCollection(COLLECTION);

    String bootstrapServers = kafkaCluster.bootstrapServers();
    log.info("bootstrapServers={}", bootstrapServers);


    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, bootstrapServers);
    properties.put(KafkaCrossDcConf.ZK_CONNECT_STRING, solrCluster2.getZkServer().getZkAddress());
    properties.put(KafkaCrossDcConf.TOPIC_NAME, TOPIC);
    properties.put(KafkaCrossDcConf.GROUP_ID, "group1");
    properties.put(KafkaCrossDcConf.MAX_POLL_RECORDS, 3);
    consumer.start(properties);

  }

  @AfterClass
  public static void afterSolrAndKafkaIntegrationTest() throws Exception {
    ObjectReleaseTracker.clear();

    consumer.shutdown();

    try {
      if (kafkaCluster != null) {
        kafkaCluster.stop();
      }
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }

    if (solrCluster1 != null) {
      solrCluster1.getZkServer().getZkClient().printLayoutToStdOut();
      solrCluster1.shutdown();
    }
    if (solrCluster2 != null) {
      solrCluster2.getZkServer().getZkClient().printLayoutToStdOut();
      solrCluster2.shutdown();
    }
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    solrCluster1.getSolrClient().deleteByQuery("*:*");
    solrCluster2.getSolrClient().deleteByQuery("*:*");
    solrCluster1.getSolrClient().commit();
    solrCluster2.getSolrClient().commit();
  }

  public void testFullCloudToCloud() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient();

    addDocs(client, "first");

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 1000; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 7) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    assertTrue("results=" + results, foundUpdates);

    QueryResponse results1 =solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("first"));
    QueryResponse results2 = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("first"));

    assertEquals("results=" + results1, 7, results1.getResults().getNumFound());
    assertEquals("results=" + results2, 7, results2.getResults().getNumFound());

    addDocs(client, "second");

    foundUpdates = false;
    for (int i = 0; i < 1000; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 7) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    System.out.println("Closed producer");

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);

    results1 =solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("second"));
    results2 = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("second"));

    assertEquals("results=" + results1, 7, results1.getResults().getNumFound());
    assertEquals("results=" + results2, 7, results2.getResults().getNumFound());

    addDocs(client, "third");

    foundUpdates = false;
    for (int i = 0; i < 1000; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 7) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    System.out.println("Closed producer");

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);

    results1 =solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("third"));
    results2 = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("third"));

    assertEquals("results=" + results1, 7, results1.getResults().getNumFound());
    assertEquals("results=" + results2, 7, results2.getResults().getNumFound());



  }

  private void addDocs(CloudSolrClient client, String tag) throws SolrServerException, IOException {
    String id1 = "1";
    String id2 = "2";
    String id3 = "3";
    String id4 = "4";
    String id5 = "5";
    String id6 = "6";
    String id7 = "7";

    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField("id", id1);
    doc1.addField("text", "some test one " + tag);

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", id2);
    doc2.addField("text", "some test two " + tag);

    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(2);
    docs.add(doc1);
    docs.add(doc2);

    client.add(docs);

    client.commit(COLLECTION);

    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField("id", id3);
    doc3.addField("text", "some test three " + tag);

    SolrInputDocument doc4 = new SolrInputDocument();
    doc4.addField("id", id4);
    doc4.addField("text", "some test four " + tag);

    SolrInputDocument doc5 = new SolrInputDocument();
    doc5.addField("id", id5);
    doc5.addField("text", "some test five " + tag);

    SolrInputDocument doc6 = new SolrInputDocument();
    doc6.addField("id", id6);
    doc6.addField("text", "some test six " + tag);

    SolrInputDocument doc7 = new SolrInputDocument();
    doc7.addField("id", id7);
    doc7.addField("text", "some test seven " + tag);

    client.add(doc3);
    client.add(doc4);
    client.add(doc5);
    client.add(doc6);
    client.add(doc7);

    client.commit(COLLECTION);
  }

}
