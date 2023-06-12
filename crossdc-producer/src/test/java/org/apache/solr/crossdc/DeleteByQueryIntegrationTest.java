package org.apache.solr.crossdc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.consumer.Consumer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.DEFAULT_MAX_REQUEST_SIZE;

@ThreadLeakFilters(defaultFilters = true, filters = {SolrIgnoredThreadsFilter.class,
        QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class})
@ThreadLeakLingering(linger = 5000)
public class DeleteByQueryIntegrationTest extends
        SolrTestCaseJ4 {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int MAX_MIRROR_BATCH_SIZE_BYTES = Integer.valueOf(DEFAULT_MAX_REQUEST_SIZE);
    private static final int MAX_DOC_SIZE_BYTES = MAX_MIRROR_BATCH_SIZE_BYTES;

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

        if (solrCluster1 != null) {
            solrCluster1.getZkServer().getZkClient().printLayoutToStdOut();
            solrCluster1.shutdown();
        }
        if (solrCluster2 != null) {
            solrCluster2.getZkServer().getZkClient().printLayoutToStdOut();
            solrCluster2.shutdown();
        }

        try {
            kafkaCluster.stop();
        } catch (Exception e) {
            log.error("Exception stopping Kafka cluster", e);
        }
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        solrCluster1.deleteAllCollections();
        solrCluster2.deleteAllCollections();
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
        solrCluster1.getSolrClient().request(create);
        solrCluster1.waitForActiveCollection(COLLECTION, 1, 1);
        solrCluster2.getSolrClient().request(create);
        solrCluster2.waitForActiveCollection(COLLECTION, 1, 1);
    }

    @Test
    public void testDeleteById() throws Exception {
        // Add document
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        SolrInputDocument doc1 = new SolrInputDocument();
        String id1 = String.valueOf(System.currentTimeMillis());
        doc1.addField("id", id1);
        doc1.addField("text", "test text 1");
        client1.add(doc1);
        client1.commit(COLLECTION);

        // Delete document by ID
        client1.deleteById(COLLECTION, id1);

        // Verify deletion in solrCluster2
        assertCluster2EventuallyHasNoDocs(COLLECTION, "id:" + id1);
    }

    @Test
    public void testDeleteByQuery() throws Exception {
        // Add document
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        SolrInputDocument doc1 = new SolrInputDocument();
        String id1 = String.valueOf(System.currentTimeMillis());
        doc1.addField("id", id1);
        doc1.addField("text", "test text 2");
        client1.add(doc1);
        client1.commit(COLLECTION);

        // Delete document by query
        client1.deleteByQuery(COLLECTION, "id:" + id1);

        // Verify deletion in solrCluster2
        assertCluster2EventuallyHasNoDocs(COLLECTION, "id:" + id1);
    }

    @Test
    public void testDeleteByQueryWithNoPaging() throws Exception {
        // Add document
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        SolrInputDocument doc1 = new SolrInputDocument();
        String id1 = String.valueOf(System.currentTimeMillis());
        doc1.addField("id", id1);
        doc1.addField("text", "test text 3");
        client1.add(doc1);
        client1.commit(COLLECTION);

        // Set dbqMethod to convert_no_paging
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("dbqMethod", "convert_no_paging");

        // Delete document by query
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setParams(params);
        updateRequest.deleteByQuery("id:" + id1);
        updateRequest.process(client1, COLLECTION);

        // Verify deletion in solrCluster2
        assertCluster2EventuallyHasNoDocs(COLLECTION, "id:" + id1);
    }

    @Test
    public void testBulkDelete() throws Exception {
        // Add several documents
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            String id = String.valueOf(System.currentTimeMillis() + i);
            ids.add(id);
            doc.addField("id", id);
            doc.addField("text", "test text " + i);
            client1.add(doc);
        }
        client1.commit(COLLECTION);

        // Delete documents by ID
        client1.deleteById(COLLECTION, ids);

        // Verify deletions in solrCluster2
        for (String id : ids) {
            assertCluster2EventuallyHasNoDocs(COLLECTION, "id:" + id);
        }
    }

    @Test
    public void testConcurrentDeletes() throws Exception {
        // Create several threads, each performing add and delete operations
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    // Perform add and delete operations
                    CloudSolrClient client = solrCluster1.getSolrClient();
                    String id = String.valueOf(System.currentTimeMillis()) + "-" + threadId;
                    SolrInputDocument doc = new SolrInputDocument();
                    doc.addField("id", id);
                    doc.addField("text", "test text " + threadId);
                    client.add(doc);
                    client.commit(COLLECTION);

                    // Delete the document
                    client.deleteById(COLLECTION, id);

                    // Check if the document is deleted in the second cluster
                    assertCluster2EventuallyHasNoDocs(COLLECTION, "id:" + id);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Shut down the executor and wait for all tasks to complete
        executor.shutdown();
        assertTrue("Tasks did not finish in time", executor.awaitTermination(5, TimeUnit.MINUTES));
    }

    @Test
    public void testDeleteNonexistentDocument() throws Exception {
        // Try to delete a document that doesn't exist
        CloudSolrClient client = solrCluster1.getSolrClient();
        String id = "nonexistent";
        client.deleteById(COLLECTION, id);

        // Check that the nonexistent document is still nonexistent in the second cluster
        assertCluster2EventuallyHasNoDocs(COLLECTION, "id:" + id);
    }

    @Test
    public void testStressDelete() throws Exception {
        // Create a large number of documents
        int docCount = 250;
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        List<String> ids = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            String id = String.valueOf(System.currentTimeMillis() + i);
            ids.add(id);
            doc.addField("id", id);
            doc.addField("text", "test text " + i);
            client1.add(doc);
        }
        client1.commit(COLLECTION);

        // Delete the documents
        for (String id : ids) {
            client1.deleteById(COLLECTION, id);
        }

        // Verify deletions in solrCluster2
        for (String id : ids) {
            assertCluster2EventuallyHasNoDocs(COLLECTION, "id:" + id);
        }
    }

    @Test
    public void testDeleteByQueryLocal() throws Exception {
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        CloudSolrClient client2 = solrCluster2.getSolrClient();

        // Add a document
        String id = String.valueOf(System.nanoTime());
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField("text", "test text");
        client1.add(doc);
        client1.commit(COLLECTION);

        assertClusterEventuallyHasDocs(client2, COLLECTION, "id:" + id, 1);

        // Delete the document locally using deleteByQuery with dbqMethod set to "delete_by_query_local"
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("dbqMethod", "delete_by_query_local");
        UpdateRequest request = new UpdateRequest();
        request.setParams(params);
        request.deleteByQuery("id:" + id);
        request.process(client1, COLLECTION);
        client1.commit(COLLECTION);

        // The document should be deleted locally
        assertClusterEventuallyHasDocs(client1, COLLECTION, "id:" + id, 0);

        Thread.sleep(1500);
        client2.commit(COLLECTION);
        // The document should not be deleted in the second cluster
        assertClusterEventuallyHasDocs(client2, COLLECTION, "id:" + id, 1);
    }

    @Test
    public void testPassDeleteByQuery() throws Exception {
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        CloudSolrClient client2 = solrCluster2.getSolrClient();

        // Add a document
        String id = String.valueOf(System.currentTimeMillis());
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField("text", "test text");
        client1.add(doc);
        client1.commit(COLLECTION);

        // Delete the document using deleteByQuery with dbqMethod set to "delete_by_query"
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("dbqMethod", "delete_by_query");
        UpdateRequest request = new UpdateRequest();
        request.setParams(params);
        request.deleteByQuery("id:" + id);
        request.process(client1, COLLECTION);
        client1.commit(COLLECTION);

        // The document should be deleted in both clusters
        assertClusterEventuallyHasDocs(client1, COLLECTION, "id:" + id, 0);
        assertClusterEventuallyHasDocs(client2, COLLECTION, "id:" + id, 0);
    }

    @Test
    public void testDeleteByQueryConvertNoPaging() throws Exception {
        CloudSolrClient client1 = solrCluster1.getSolrClient();
        CloudSolrClient client2 = solrCluster2.getSolrClient();

        // Add a document
        String id = String.valueOf(System.currentTimeMillis());
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField("text", "test text");
        client1.add(doc);
        client1.commit(COLLECTION);

        // Delete the document using deleteByQuery with dbqMethod set to "convert_no_paging"
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("dbqMethod", "convert_no_paging");
        UpdateRequest request = new UpdateRequest();
        request.setParams(params);
        request.deleteByQuery("id:" + id);
        request.process(client1, COLLECTION);
        client1.commit(COLLECTION);

        // The document should be deleted in both clusters
        assertClusterEventuallyHasDocs(client1, COLLECTION, "id:" + id, 0);
        assertClusterEventuallyHasDocs(client2, COLLECTION, "id:" + id, 0);
    }

    private void assertCluster2EventuallyHasNoDocs(String collection, String query) throws Exception {
        assertClusterEventuallyHasDocs(solrCluster2.getSolrClient(), collection, query, 0);
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
        for (int i = 0; i < 500; i++) {
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
