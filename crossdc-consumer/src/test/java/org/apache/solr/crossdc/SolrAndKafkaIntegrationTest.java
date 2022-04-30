package org.apache.solr.crossdc;

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

import static org.mockito.Mockito.spy;

@ThreadLeakFilters(
    defaultFilters = true,
    filters = { SolrIgnoredThreadsFilter.class, QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class})
public class SolrAndKafkaIntegrationTest extends SolrCloudTestCase {
  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

  protected static volatile MiniSolrCloudCluster cluster1;
  protected static volatile MiniSolrCloudCluster cluster2;
  private static SolrMessageProcessor processor;

  private static ResubmitBackoffPolicy backoffPolicy = spy(new TestMessageProcessor.NoOpResubmitBackoffPolicy());

  @BeforeClass
  public static void setupIntegrationTest() throws Exception {

    CLUSTER.start();

    cluster1 =
        new Builder(2, createTempDir())
            .addConfig("conf", getFile("src/resources/configs/cloud-minimal/conf").toPath())
            .configure();

    processor = new SolrMessageProcessor(cluster1.getSolrClient(), backoffPolicy);
  }

  @AfterClass
  public static void tearDownIntegrationTest() throws Exception {

    CLUSTER.stop();

    if (cluster1 != null) {
      cluster1.shutdown();
    }
    if (cluster2 != null) {
      cluster2.shutdown();
    }
  }

  public void test() {

  }
}
