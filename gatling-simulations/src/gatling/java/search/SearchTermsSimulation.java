package search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.gatling.javaapi.core.ChainBuilder;
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

public class SearchTermsSimulation extends Simulation {

    private final String testWorkDir;
    private final String collectionName;
    private final Path termsFile;
    private final int atOnceUsersCount;
    private final HttpProtocolBuilder httpProtocol;
    private final ChainBuilder searchChain;
    private final ScenarioBuilder scn;
    private final String queryParams;

    public SearchTermsSimulation() {
        atOnceUsersCount = getConfigInt("CONCURRENT_USERS", 10);
        collectionName = getConfig("COLLECTION_NAME", "wikipedia");
        testWorkDir = getConfig("TESTS_WORK_DIR", ".gatling");
        String termsFileName = getConfig("SEARCH_TERMS_FILE", "wikipedia-queries.txt");
        queryParams = getConfig("QUERY_PARAMS","");
        termsFile = Paths.get(testWorkDir, termsFileName);

        httpProtocol = http.baseUrl(GatlingUtils.getEndpoint());

        List<String> terms = readTerms(termsFile);
        if (terms.isEmpty()) {
            System.out.printf("No search terms found in %s; exiting simulation setup.%n", termsFile);
        }
        else{
            System.out.printf("Found %s terms.%n", terms.size());
        }

        searchChain = search(terms);

        int iterations = Math.max(1, terms.size() / Math.max(1, atOnceUsersCount));

        scn = scenario(this.getClass().getSimpleName())
                .repeat(iterations)
                .on(exec(searchChain));

        this.setUp(scn.injectOpen(atOnceUsers(atOnceUsersCount))).protocols(httpProtocol);
    }

    public static String getConfig(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, System.getProperty(key, defaultValue));
    }

    public static int getConfigInt(String key, int defaultValue) {
        return Integer.parseInt(getConfig(key, String.valueOf(defaultValue)));
    }

    private static List<String> readTerms(Path file) {
        if (!Files.exists(file)) return Collections.emptyList();
        List<String> terms = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file)) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) terms.add(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return terms;
    }

    private ChainBuilder search(List<String> terms) {


        Iterator<Map<String, Object>> feeder = terms.stream()
                .map(t -> Collections.singletonMap("term", (Object) t))
                .iterator();

        HttpRequestActionBuilder req = http("search")
                .get("/solr/" + collectionName + "/select")
                .queryParam("q", "#{term}")
                .queryParam("wt", "json");

        // Add any extra query parameters provided as a single string: key1=val1&flag=true
        if (!queryParams.isBlank()) {
            for (String pair : queryParams.split("&")) {
                if (pair.isBlank()) continue;
                int idx = pair.indexOf('=');
                if (idx < 0) {
                    req = req.queryParam(pair.trim(), "");
                } else {
                    String key = pair.substring(0, idx).trim();
                    String value = pair.substring(idx + 1).trim();
                    req = req.queryParam(key, value);
                }
            }
        }

        return feed(feeder).exec(req);
    }

}
