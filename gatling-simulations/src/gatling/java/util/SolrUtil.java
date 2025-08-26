package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class SolrUtil {
    private static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);

    public static void createCollection(String endpoint, String collectionName, int numShards, int numReplicas, String configName) throws IOException {
        URL url = new URL(endpoint + "/admin/collections?action=create&maxShardsPerNode=-1&name="+ collectionName
                + "&numShards=" + numShards
                + "&nrtReplicas=" + numReplicas
                + "&waitForFinalState=true"
                + "&collection.configName=" + configName);
        sendGet(url, "create", true);
    }

    public static void deleteCollection(String endpoint, String collectionName) throws IOException {
        deleteCollection(endpoint, collectionName, true);
    }

    public static void deleteCollection(String endpoint, String collectionName, boolean logError) throws IOException {
        URL url = new URL(endpoint + "/admin/collections?action=delete&name=" + collectionName);
        sendGet(url, "delete", logError);
    }

    private static void sendGet(URL url, String action, boolean logError) throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET"); // This is default method, but be explicit anyway
        con.setConnectTimeout(10 * 1000);
        con.setReadTimeout(600 * 1000);
        int responseCode = con.getResponseCode();
        if (logError && responseCode != HttpURLConnection.HTTP_OK) {
            try (InputStream err = con.getErrorStream()) {
                String response = "";
                if (err != null) { // code >= 400
                    response = new String(err.readAllBytes(), StandardCharsets.UTF_8);
                }
                logger.error("Unable to {} collection; {}", action, response);
            }
            throw new RuntimeException("Unable to " + action + " collection: Response Code:" + responseCode);
        }
        con.disconnect();
    }
}
