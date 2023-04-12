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
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
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
import org.junit.*;
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
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.INDEX_UNMIRRORABLE_DOCS;
import static org.mockito.Mockito.spy;

@ThreadLeakFilters(defaultFilters = true, filters = { SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class })
@ThreadLeakLingering(linger = 5000) public class SolrAndKafkaIntegrationTest extends
    SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_DOC_SIZE_BYTES = Integer.parseInt(DEFAULT_MAX_REQUEST_SIZE);

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
  protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer;

  private static String TOPIC = "topic1";

  private static String COLLECTION = "collection1";
  private static String ALT_COLLECTION = "collection2";
  private static Thread.UncaughtExceptionHandler uceh;

  @Before
  public void beforeSolrAndKafkaIntegrationTest() throws Exception {
    uceh = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
      log.error("Uncaught exception in thread " + t, e);
    });
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
      // kafkaCluster.deleteAllTopicsAndWait(5000);
      kafkaCluster.stop();
      kafkaCluster = null;
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }

    Thread.setDefaultUncaughtExceptionHandler(uceh);
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

  private static SolrInputDocument getDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");
    return doc;
  }

  public void testProducerToCloud() throws Exception {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", kafkaCluster.bootstrapServers());
    properties.put("acks", "all");
    properties.put("retries", 1);
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
    tooLargeDoc.addField("id", System.nanoTime());
    tooLargeDoc.addField("text", new String(new byte[2 * MAX_DOC_SIZE_BYTES]));
    final SolrInputDocument normalDoc = new SolrInputDocument();
    normalDoc.addField("id", System.nanoTime() + 22);
    normalDoc.addField("text", "Hello world");
    List<SolrInputDocument> docsToIndex = new ArrayList<>();
    docsToIndex.add(normalDoc);
    docsToIndex.add(tooLargeDoc);

    final CloudSolrClient cluster1Client = solrCluster1.getSolrClient();
    try {
      cluster1Client.add(docsToIndex);
    } catch (BaseCloudSolrClient.RouteException e) {
      // expected
    }
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
    try {
      solrCluster1.getSolrClient().request(create);
      solrCluster2.getSolrClient().request(create);
      solrCluster1.waitForActiveCollection(ALT_COLLECTION, 1, 1);
      solrCluster2.waitForActiveCollection(ALT_COLLECTION, 1, 1);

      cluster1Client.add(ALT_COLLECTION, docsToIndex);
      cluster1Client.commit(ALT_COLLECTION);

      // try adding another doc
//      final SolrInputDocument newDoc = new SolrInputDocument();
//
//      newDoc.addField("id", System.nanoTime());
//      newDoc.addField("text", "Hello world");
//      docsToIndex = new ArrayList<>();
//      docsToIndex.add(newDoc);
//
//    try {
//      cluster1Client.add(ALT_COLLECTION, docsToIndex);
//    } catch (BaseCloudSolrClient.RouteException e) {
//      // expected
//    }
//      cluster1Client.commit(ALT_COLLECTION);

      // Primary should have both 'normal' and 'large' docs; secondary should only have 'normal' doc.
      assertClusterEventuallyHasDocs(cluster1Client, ALT_COLLECTION, "*:*", 2);
      assertCluster2EventuallyHasDocs(ALT_COLLECTION, "*:*", 1);
      assertCluster2EventuallyHasDocs(ALT_COLLECTION, normalDocQuery, 1);
    } finally {
      CollectionAdminRequest.Delete delete =
        CollectionAdminRequest.deleteCollection(ALT_COLLECTION);
      solrCluster1.getSolrClient().request(delete);
      solrCluster2.getSolrClient().request(delete);
    }
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
        Thread.sleep(200);
      }
    }

    assertTrue("results=" + results, foundUpdates);
  }
}
