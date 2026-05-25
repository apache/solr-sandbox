package index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.StreamSupport;

import io.gatling.javaapi.core.ChainBuilder;
import static io.gatling.javaapi.core.CoreDsl.RawFileBody;
import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.exec;
import static io.gatling.javaapi.core.CoreDsl.feed;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import static io.gatling.javaapi.http.HttpDsl.http;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import io.gatling.javaapi.http.HttpRequestActionBuilder;
import util.GatlingUtils;
import util.SolrUtil;

public class IndexWikipediaBatchesSimulation extends Simulation {

    private final String testWorkDir;
    private final Path batchesDir;
    private final int numFiles;
    private final int numBatches;
    private final int atOnceUsersCount;
    private final boolean tearDownCollection;
    private final HttpProtocolBuilder httpProtocol;
    private final ChainBuilder updates;
    private final ScenarioBuilder scn;

    public IndexWikipediaBatchesSimulation() {
        atOnceUsersCount = getConfigInt("CONCURRENT_USERS", 10);

        testWorkDir = getConfig("TESTS_WORK_DIR", ".gatling");
        tearDownCollection = getConfigBool("TEAR_DOWN_COLLECTION", true);
        numBatches = getConfigInt("NUM_BATCHES", -1);
        batchesDir = Paths.get(testWorkDir, "batches");
        numFiles = numBatches > -1 ? numBatches : batchesDir.toFile().list().length;
        httpProtocol =  http.baseUrl(GatlingUtils.getEndpoint());
        updates = index(testWorkDir);
        scn = scenario(this.getClass().getSimpleName())
                .repeat(getIterations(numFiles, atOnceUsersCount))
                .on(exec(updates));
        this.setUp(scn.injectOpen(atOnceUsers(atOnceUsersCount))).protocols(httpProtocol);
    }

    public int getIterations(int numBatchesAvailable, int numAtOnceUsers) {
        return numBatchesAvailable / numAtOnceUsers;
    }

    public static String getConfig(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, System.getProperty(key, defaultValue));
    }

    public static int getConfigInt(String key, int defaultValue) {
        return Integer.parseInt(getConfig(key, String.valueOf(defaultValue)));
    }

    public static boolean getConfigBool(String key, boolean defaultValue) {
        return Boolean.parseBoolean(getConfig(key, String.valueOf(defaultValue)));
    }

    @Override
    public void before() {
        setupCollection();
    }

    @Override
    public void after() {
        if (tearDownCollection) {
            tearDownCollection();
        }
    }


    public void setupCollection() {
        String endpoint = GatlingUtils.getEndpoint();
        String collectionName = getConfig("COLLECTION_NAME", "wikipedia");
        int numShards = getConfigInt("NUM_SHARDS", 1);
        int numReplicas = getConfigInt("NUM_REPLICAS", 1);

        try {
            SolrUtil.deleteCollection(endpoint + "/solr", collectionName, false);
            System.out.println("Creating collection " + collectionName);
            SolrUtil.createCollection(endpoint + "/solr", collectionName, numShards, numReplicas, "wikipedia");
        } catch (Exception e) {
            System.out.printf("Wikipedia collection %s could not be created. %s: %s%n", collectionName, e.getClass(), e.getMessage());
        }

    }

    public void tearDownCollection() {
        String endpoint = GatlingUtils.getEndpoint();
        String collectionName = getConfig("COLLECTION_NAME", "wikipedia");
        System.out.println("Deleting collection " + collectionName);
        try {
            SolrUtil.deleteCollection(endpoint + "/solr", collectionName, true);
        } catch (Exception e) {
            System.out.printf("Wikipedia Collection %s could not be deleted. %s: %s%n", collectionName, e.getClass(), e.getMessage());
        }
    }

    public ChainBuilder index(String workDir) {
        String collectionName = getConfig("COLLECTION_NAME", "wikipedia");
        String contentType = getConfig("BATCH_CONTENT_TYPE", "application/json");
        Map<String, String> headers = Collections.singletonMap("Content-Type", contentType);
        Path batchesDir = Paths.get(workDir, "batches");

        Iterator<Map<String, Object>> documents = StreamSupport.stream(newDirectoryStream(batchesDir).spliterator(), false)
                .map(path -> Collections.singletonMap("file", (Object) path))
                .iterator();

        HttpRequestActionBuilder updateReq = http("updates")
                .post("/solr/" + collectionName + "/update")
                .headers(headers);

        return feed(documents).exec(updateReq.body(RawFileBody("#{file}")));
    }

    private static DirectoryStream<Path> newDirectoryStream(Path dir) {
        try {
            return Files.newDirectoryStream(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}