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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
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

@ThreadLeakFilters(
    defaultFilters = true,
    filters = { SolrIgnoredThreadsFilter.class, QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class})
@ThreadLeakLingering(linger = 5000)
public class SolrAndKafkaIntegrationTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
 //protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer = new Consumer();

  private static String TOPIC = "topic1";
  
  private static String COLLECTION = "collection1";


  @BeforeClass
  public static void setupIntegrationTest() throws Exception {

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

    solrCluster1 =
        new Builder(2, createTempDir())
            .addConfig("conf", getFile("src/test/resources/configs/cloud-minimal/conf").toPath())
            .configure();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(COLLECTION, 1, 1);

    solrCluster1.getSolrClient().setDefaultCollection(COLLECTION);

    String bootstrapServers = kafkaCluster.bootstrapServers();
    log.info("bootstrapServers={}", bootstrapServers);

    consumer.start(bootstrapServers, solrCluster1.getZkServer().getZkHost(), TOPIC, false, 0);


  }

  @AfterClass
  public static void tearDownIntegrationTest() throws Exception {
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
   // if (solrCluster2 != null) {
   //   solrCluster2.shutdown();
    //}
  }

  public void test() throws Exception {

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
    updateRequest.add("id", String.valueOf(System.currentTimeMillis()));
    updateRequest.add("text", "test");
    updateRequest.setParam("collection", COLLECTION);
    MirroredSolrRequest mirroredSolrRequest = new MirroredSolrRequest(updateRequest);
    System.out.println("About to send producer record");
    producer.send(new ProducerRecord(TOPIC, mirroredSolrRequest), (metadata, exception) -> {
      log.info("Producer finished sending metadata={}, exception={}", metadata, exception);
    });
    producer.flush();
    System.out.println("Sent producer record");
    producer.close();
    System.out.println("Closed producer");

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 10; i++) {
      results = solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 1) {
        foundUpdates = true;
      } else {
        Thread.sleep(500);
      }


    }

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);


  }
}
