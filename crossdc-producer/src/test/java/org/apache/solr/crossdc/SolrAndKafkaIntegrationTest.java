package org.apache.solr.crossdc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.crossdc.consumer.Consumer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.sys.Prop;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.DEFAULT_MAX_REQUEST_SIZE;
import static org.mockito.Mockito.spy;

@ThreadLeakFilters(defaultFilters = true, filters = { SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class })
@ThreadLeakLingering(linger = 5000) public class SolrAndKafkaIntegrationTest extends
    SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_DOC_SIZE_BYTES = Integer.valueOf(DEFAULT_MAX_REQUEST_SIZE);

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
  protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer = new Consumer();

  private static String TOPIC = "topic1";

  private static String COLLECTION = "collection1";
  private static String ALT_COLLECTION = "collection2";

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

  @AfterClass
  public static void afterSolrAndKafkaIntegrationTest() throws Exception {
    ObjectReleaseTracker.clear();

    consumer.shutdown();

    try {
      kafkaCluster.stop();
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

    // Delete alternate collection in case it was created by any tests.
    if (CollectionAdminRequest.listCollections(solrCluster1.getSolrClient()).contains(ALT_COLLECTION)) {
      solrCluster1.getSolrClient().request(CollectionAdminRequest.deleteCollection(ALT_COLLECTION));
      solrCluster2.getSolrClient().request(CollectionAdminRequest.deleteCollection(ALT_COLLECTION));
    }
  }

  public void testFullCloudToCloud() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.currentTimeMillis()));
    doc.addField("text", "some test");

    client.add(doc);

    client.commit(COLLECTION);

    System.out.println("Sent producer record");

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 1);
  }

  public void testProducerToCloud() throws Exception {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", kafkaCluster.bootstrapServers());
    properties.put("acks", "all");
    properties.put("retries", 0);
    properties.put("batch.size", 1);
    properties.put("buffer.memory", 33554432);
    properties.put("linger.ms", 1);
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", MirroredSolrRequestSerializer.class.getName());
    Producer<String, MirroredSolrRequest> producer = new KafkaProducer(properties);
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setParam("shouldMirror", "true");
    updateRequest.add("id", String.valueOf(System.currentTimeMillis()), "text", "test");
    updateRequest.add("id", String.valueOf(System.currentTimeMillis() + 22), "text", "test2");
    updateRequest.setParam("collection", COLLECTION);
    MirroredSolrRequest mirroredSolrRequest = new MirroredSolrRequest(updateRequest);
    System.out.println("About to send producer record");
    producer.send(new ProducerRecord(TOPIC, mirroredSolrRequest), (metadata, exception) -> {
      log.info("Producer finished sending metadata={}, exception={}", metadata, exception);
    });
    producer.flush();

    System.out.println("Sent producer record");

    solrCluster2.getSolrClient().commit(COLLECTION);

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 2);

    producer.close();
  }

  @Test
  public void testMirroringUpdateProcessor() throws Exception {
    final SolrInputDocument tooLargeDoc = new SolrInputDocument();
    tooLargeDoc.addField("id", "tooLarge-" + String.valueOf(System.currentTimeMillis()));
    tooLargeDoc.addField("text", new String(new byte[2 * MAX_DOC_SIZE_BYTES]));
    final SolrInputDocument normalDoc = new SolrInputDocument();
    normalDoc.addField("id", "normalDoc-" + String.valueOf(System.currentTimeMillis()));
    normalDoc.addField("text", "Hello world");
    final List<SolrInputDocument> docsToIndex = new ArrayList<>();
    docsToIndex.add(tooLargeDoc);
    docsToIndex.add(normalDoc);

    final CloudSolrClient cluster1Client = solrCluster1.getSolrClient();
    cluster1Client.add(docsToIndex);
    cluster1Client.commit(COLLECTION);

    // Primary and secondary should each only index 'normalDoc'
    final String normalDocQuery = "id:" + normalDoc.get("id").getFirstValue();
    assertCluster2EventuallyHasDocs(COLLECTION, normalDocQuery, 1);
    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 1);
    assertClusterEventuallyHasDocs(cluster1Client, COLLECTION, normalDocQuery, 1);
    assertClusterEventuallyHasDocs(cluster1Client, COLLECTION, "*:*", 1);

    // Create new primary+secondary collection where 'tooLarge' docs ARE indexed on the primary
    CollectionAdminRequest.Create create =
            CollectionAdminRequest.createCollection(ALT_COLLECTION, "conf", 1, 1)
                    .withProperty("indexUnmirrorableDocs", "true");
    solrCluster1.getSolrClient().request(create);
    solrCluster2.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(ALT_COLLECTION, 1, 1);
    solrCluster2.waitForActiveCollection(ALT_COLLECTION, 1, 1);

    cluster1Client.add(ALT_COLLECTION, docsToIndex);
    cluster1Client.commit(ALT_COLLECTION);

    // Primary should have both 'normal' and 'large' docs; secondary should only have 'normal' doc.
    assertClusterEventuallyHasDocs(cluster1Client, ALT_COLLECTION, "*:*", 2);
    assertCluster2EventuallyHasDocs(ALT_COLLECTION, normalDocQuery, 1);
    assertCluster2EventuallyHasDocs(ALT_COLLECTION, "*:*", 1);
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

  private void assertClusterEventuallyHasDocs(SolrClient client, String collection, String query, int expectedNumDocs) throws Exception {
    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 100; i++) {
      client.commit(collection);
      results = client.query(collection, new SolrQuery(query));
      if (results.getResults().getNumFound() == expectedNumDocs) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    assertTrue("results=" + results, foundUpdates);
  }
}
