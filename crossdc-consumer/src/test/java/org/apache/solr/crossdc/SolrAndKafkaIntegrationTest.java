package org.apache.solr.crossdc;

<<<<<<< refs/remotes/markrmiller/itwip
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.crossdc.consumer.Consumer;
import org.apache.solr.crossdc.messageprocessor.SolrMessageProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Properties;
=======
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.messageprocessor.SolrMessageProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
>>>>>>> Add back in the EmbeddedKafkaCluster.

import static org.mockito.Mockito.spy;

@ThreadLeakFilters(
    defaultFilters = true,
    filters = { SolrIgnoredThreadsFilter.class, QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class})
<<<<<<< refs/remotes/markrmiller/itwip
@ThreadLeakAction(ThreadLeakAction.Action.INTERRUPT)
public class SolrAndKafkaIntegrationTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
  protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer = new Consumer();

  private static String TOPIC = "topic1";
  
  private static String COLLECTION = "collection1";
=======
public class SolrAndKafkaIntegrationTest extends SolrCloudTestCase {
  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

  protected static volatile MiniSolrCloudCluster cluster1;
  protected static volatile MiniSolrCloudCluster cluster2;
  private static SolrMessageProcessor processor;
>>>>>>> Add back in the EmbeddedKafkaCluster.

  private static ResubmitBackoffPolicy backoffPolicy = spy(new TestMessageProcessor.NoOpResubmitBackoffPolicy());

  @BeforeClass
  public static void setupIntegrationTest() throws Exception {
<<<<<<< refs/remotes/markrmiller/itwip
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

    consumer.start(bootstrapServers, solrCluster1.getZkServer().getZkAddress(), TOPIC, false, 0);

=======

    CLUSTER.start();

    cluster1 =
        new Builder(2, createTempDir())
            .addConfig("conf", getFile("src/resources/configs/cloud-minimal/conf").toPath())
            .configure();

    processor = new SolrMessageProcessor(cluster1.getSolrClient(), backoffPolicy);
>>>>>>> Add back in the EmbeddedKafkaCluster.
  }

  @AfterClass
  public static void tearDownIntegrationTest() throws Exception {
<<<<<<< refs/remotes/markrmiller/itwip
    consumer.shutdown();

    try {
      kafkaCluster.stop();
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }

    if (solrCluster1 != null) {
      solrCluster1.shutdown();
    }
    if (solrCluster2 != null) {
      solrCluster2.shutdown();
    }
  }

  public void test() throws InterruptedException {
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

    Thread.sleep(10000);
=======

    CLUSTER.stop();

    if (cluster1 != null) {
      cluster1.shutdown();
    }
    if (cluster2 != null) {
      cluster2.shutdown();
    }
  }

  public void test() {

>>>>>>> Add back in the EmbeddedKafkaCluster.
  }
}
