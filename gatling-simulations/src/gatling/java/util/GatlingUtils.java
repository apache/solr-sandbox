package util;

public class GatlingUtils {

    public static String getEndpoint() {
        String endpoint = System.getProperty("endpoint");
        //val endpoint = "http://localhost:8983";
        if (endpoint == null) {
            throw new IllegalArgumentException("Missing endpoint. Please provide \"endpoint\" system property");
        }
        if (endpoint.endsWith("/solr")) {
            throw new IllegalArgumentException("Endpoint should not include Solr path");
        }
        if (endpoint.endsWith("/")) {
            throw new IllegalArgumentException("Endpoint should not end with trailing /");
        }
        return endpoint;
    }
}