package org.apache.solr.crossdc;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.ResubmitBackoffPolicy;
import org.apache.solr.crossdc.messageprocessor.SolrMessageProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Mockito.spy;

public class IntegrationTest extends SolrCloudTestCase {
  static final String VERSION_FIELD = "_version_";

  protected static volatile MiniSolrCloudCluster cluster1;
  protected static volatile MiniSolrCloudCluster cluster2;
  private static SolrMessageProcessor processor;

  private static ResubmitBackoffPolicy backoffPolicy = spy(new TestMessageProcessor.NoOpResubmitBackoffPolicy());

  @BeforeClass
  public static void setupIntegrationTest() throws Exception {

    cluster1 =
        new SolrCloudTestCase.Builder(2, createTempDir())
            .addConfig("conf", getFile("src/resources/configs/cloud-minimal/conf").toPath())
            .configure();

    processor = new SolrMessageProcessor(cluster1.getSolrClient(), backoffPolicy);
  }

  @AfterClass
  public static void tearDownIntegrationTest() throws Exception {
    if (cluster != null) {
      cluster1.shutdown();
    }
  }

  public void testDocumentSanitization() {
    UpdateRequest request = spy(new UpdateRequest());

    // Add docs with and without version
    request.add(new SolrInputDocument() {
      {
        setField("id", 1);
        setField(VERSION_FIELD, 1);
      }
    });
    request.add(new SolrInputDocument() {
      {
        setField("id", 2);
      }
    });

    // Delete by id with and without version
    request.deleteById("1");
    request.deleteById("2", 10L);

    request.setParam("shouldMirror", "true");
    // The response is irrelevant, but it will fail because mocked server returns null when processing
    processor.handleItem(new MirroredSolrRequest(request));

    // After processing, check that all version fields are stripped
    for (SolrInputDocument doc : request.getDocuments()) {
      assertNull("Doc still has version", doc.getField(VERSION_FIELD));
    }

    // Check versions in delete by id
    for (Map<String, Object> idParams : request.getDeleteByIdMap().values()) {
      if (idParams != null) {
        idParams.put(UpdateRequest.VER, null);
        assertNull("Delete still has version", idParams.get(UpdateRequest.VER));
      }
    }
  }

  @Test
  public void TestMethod() {

  }
}
