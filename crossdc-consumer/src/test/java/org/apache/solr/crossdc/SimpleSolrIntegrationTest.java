package org.apache.solr.crossdc;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
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

public class SimpleSolrIntegrationTest extends SolrCloudTestCase {
  static final String VERSION_FIELD = "_version_";


  protected static volatile MiniSolrCloudCluster cluster1;

  private static SolrMessageProcessor processor;

  @BeforeClass
  public static void setupIntegrationTest() throws Exception {

    cluster1 =
        new SolrCloudTestCase.Builder(2, createTempDir())
            .addConfig("conf", getFile("src/test/resources/configs/cloud-minimal/conf").toPath())
            .configure();

    String collection = "collection1";
    CloudSolrClient cloudClient1 = cluster1.getSolrClient();

    processor = new SolrMessageProcessor(cloudClient1, null);

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collection, "conf", 1, 1);
    cloudClient1.request(create);
    cluster1.waitForActiveCollection(collection, 1, 1);

    cloudClient1.setDefaultCollection(collection);
  }

  @AfterClass
  public static void tearDownIntegrationTest() throws Exception {
    if (cluster1 != null) {
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
}
