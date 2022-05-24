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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.crossdc.consumer.Consumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

import static org.mockito.Mockito.spy;

@ThreadLeakFilters(defaultFilters = true, filters = { SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class })
@ThreadLeakLingering(linger = 5000) public class SolrAndKafkaIntegrationTest extends
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

  @BeforeClass public static void setupIntegrationTest() throws Exception {

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

    consumer.start(bootstrapServers, solrCluster2.getZkServer().getZkAddress(), TOPIC, false, 0);

  }

  @AfterClass public static void tearDownIntegrationTest() throws Exception {
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
    //ObjectReleaseTracker.clear();
    // if (solrCluster2 != null) {
    //   solrCluster2.shutdown();
    //}
  }

  public void testFullCloudToCloud() throws Exception {
    Thread.sleep(10000); // TODO why?

    CloudSolrClient client = solrCluster1.getSolrClient();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.currentTimeMillis()));
    doc.addField("text", "some test");

    client.add(doc);

    client.commit(COLLECTION);

    System.out.println("Sent producer record");

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 50; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 1) {
        foundUpdates = true;
      } else {
        Thread.sleep(500);
      }
    }

    //producer.close();
    System.out.println("Closed producer");

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);

  }

  public void testProducerToCloud() throws Exception {
    Thread.sleep(10000);
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

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 50; i++) {
     // solrCluster1.getSolrClient().commit(COLLECTION);
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 1) {
        foundUpdates = true;
      } else {
        Thread.sleep(500);
      }
    }

    System.out.println("Closed producer");

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);

    producer.close();
  }
}
