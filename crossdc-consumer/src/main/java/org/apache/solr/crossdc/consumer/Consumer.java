/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crossdc.consumer;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.messageprocessor.SolrMessageProcessor;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.*;

// Cross-DC Consumer main class
public class Consumer {
    public static final String DEFAULT_PORT = "8090";
    public static final String TOPIC_NAME = "topicName";
    public static final String GROUP_ID = "groupId";
    public static final String PORT = "port";
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    private static final String DEFAULT_GROUP_ID = "SolrCrossDCConsumer";
    private static final String MAX_POLL_RECORDS = "maxPollRecords";
    public static final String DEFAULT_MAX_POLL_RECORDS = "500";

    private static final String DEFAULT_FETCH_MIN_BYTES = "512000";
    private static final String DEFAULT_FETCH_MAX_WAIT_MS = "1000";

    private static boolean enabled = true;

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * ExecutorService to manage the cross-dc consumer threads.
     */
    private ExecutorService consumerThreadExecutor;

    private Server server;
    CrossDcConsumer crossDcConsumer;

    public void start(String bootstrapServers, String zkConnectString, String topicName, String groupId, int maxPollRecords, int batchSizeBytes, int bufferMemoryBytes, int lingerMs, int requestTimeout,
        int fetchMinBytes, int fetchMaxWaitMS, boolean enableDataEncryption, int port) {
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrapServers config was not passed at startup");
        }
        if (zkConnectString == null) {
            throw new IllegalArgumentException("zkConnectString config was not passed at startup");
        }
        if (topicName == null) {
            throw new IllegalArgumentException("topicName config was not passed at startup");
        }

        if (maxPollRecords == -1) {
            maxPollRecords = Integer.parseInt(DEFAULT_MAX_POLL_RECORDS);
        }

        if (batchSizeBytes == -1) {
            batchSizeBytes = Integer.parseInt(DEFAULT_BATCH_SIZE_BYTES);
        }

        if (bufferMemoryBytes == -1) {
            bufferMemoryBytes = Integer.parseInt(DEFAULT_BUFFER_MEMORY_BYTES);
        }        
        
        if (lingerMs == -1) {
            lingerMs = Integer.parseInt(DEFAULT_LINGER_MS);
        }

        if (requestTimeout == -1) {
            requestTimeout = Integer.parseInt(DEFAULT_REQUEST_TIMEOUT);
        }

        if (fetchMinBytes == -1) {
            fetchMinBytes = Integer.parseInt(DEFAULT_FETCH_MIN_BYTES);
        }

        if (fetchMaxWaitMS == -1) {
            fetchMaxWaitMS = Integer.parseInt(DEFAULT_FETCH_MAX_WAIT_MS);
        }

        //server = new Server();
        //ServerConnector connector = new ServerConnector(server);
        //connector.setPort(port);
        //server.setConnectors(new Connector[] {connector})

        crossDcConsumer = getCrossDcConsumer(bootstrapServers, zkConnectString, topicName, groupId, maxPollRecords, batchSizeBytes, bufferMemoryBytes, lingerMs,
            requestTimeout, fetchMinBytes, fetchMaxWaitMS, enableDataEncryption);

        // Start consumer thread

        log.info("Starting CrossDC Consumer bootstrapServers={}, zkConnectString={}, topicName={}, groupId={}, enableDataEncryption={}", bootstrapServers, zkConnectString, topicName, groupId, enableDataEncryption);

        consumerThreadExecutor = Executors.newSingleThreadExecutor();
        consumerThreadExecutor.submit(crossDcConsumer);

        // Register shutdown hook
        Thread shutdownHook = new Thread(() -> System.out.println("Shutting down consumers!"));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private CrossDcConsumer getCrossDcConsumer(String bootstrapServers, String zkConnectString, String topicName, String groupId, int maxPollRecords,
        int batchSizeBytes, int bufferMemoryBytes, int lingerMs, int requestTimeout, int fetchMinBytes, int fetchMaxWaitMS, boolean enableDataEncryption) {

        KafkaCrossDcConf conf = new KafkaCrossDcConf(bootstrapServers, topicName, groupId, maxPollRecords, batchSizeBytes, bufferMemoryBytes, lingerMs,
            requestTimeout, fetchMinBytes, fetchMaxWaitMS, enableDataEncryption, zkConnectString);
        return new KafkaCrossDcConsumer(conf);
    }

    public static void main(String[] args) {

        String zkConnectString = System.getProperty("zkConnectString");
        if (zkConnectString == null || zkConnectString.isBlank()) {
            throw new IllegalArgumentException("zkConnectString not specified for producer");
        }

        String bootstrapServers = System.getProperty(BOOTSTRAP_SERVERS);
        // boolean enableDataEncryption = Boolean.getBoolean("enableDataEncryption");
        String topicName = System.getProperty(TOPIC_NAME);
        String port = System.getProperty(PORT);
        String groupId = System.getProperty(GROUP_ID, "");
        String maxPollRecords = System.getProperty("maxPollRecords");
        String batchSizeBytes = System.getProperty("batchSizeBytes");
        String bufferMemoryBytes = System.getProperty("bufferMemoryBytes");
        String lingerMs = System.getProperty("lingerMs");
        String requestTimeout = System.getProperty("requestTimeout");
        String fetchMinBytes = System.getProperty("fetchMinBytes");
        String fetchMaxWaitMS = System.getProperty("fetchMaxWaitMS");

        try (SolrZkClient client = new SolrZkClient(zkConnectString, 15000)) {

            try {
                if ((topicName == null || topicName.isBlank()) || (groupId == null || groupId.isBlank())
                    || (bootstrapServers == null || bootstrapServers.isBlank()) || (port == null || port.isBlank()) || (maxPollRecords == null || maxPollRecords.isBlank())
                    || (batchSizeBytes == null || batchSizeBytes.isBlank()) || (bufferMemoryBytes == null || bufferMemoryBytes.isBlank()) || (lingerMs == null || lingerMs.isBlank())
                    || (requestTimeout == null || requestTimeout.isBlank())  || (fetchMinBytes == null || fetchMinBytes.isBlank())  || (fetchMaxWaitMS == null || fetchMaxWaitMS.isBlank())
                    && client.exists(System.getProperty(CrossDcConf.ZK_CROSSDC_PROPS_PATH, KafkaCrossDcConf.CROSSDC_PROPERTIES), true)) {
                    byte[] data = client.getData(System.getProperty(CrossDcConf.ZK_CROSSDC_PROPS_PATH, KafkaCrossDcConf.CROSSDC_PROPERTIES), null, null, true);
                    Properties props = new Properties();
                    props.load(new ByteArrayInputStream(data));

                    topicName = getConfig(TOPIC_NAME, topicName, props);
                    bootstrapServers = getConfig(BOOTSTRAP_SERVERS, bootstrapServers, props);
                    port = getConfig(PORT, port, props);
                    groupId = getConfig(GROUP_ID, groupId, props);
                    maxPollRecords = getConfig(MAX_POLL_RECORDS, maxPollRecords, props);
                    batchSizeBytes = getConfig("batchSizeBytes", batchSizeBytes, props);
                    bufferMemoryBytes = getConfig("bufferMemoryBytes", bufferMemoryBytes, props);
                    lingerMs = getConfig("lingerMs", lingerMs, props);
                    requestTimeout = getConfig("requestTimeout", requestTimeout, props);
                    fetchMinBytes = getConfig("fetchMinBytes", fetchMinBytes, props);
                    fetchMaxWaitMS = getConfig("fetchMaxWaitMS", fetchMaxWaitMS, props);

                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
            } catch (Exception e) {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            }
        }

        if (port == null) {
            port = DEFAULT_PORT;
        }

        if (bootstrapServers == null || bootstrapServers.isBlank()) {
          throw new IllegalArgumentException("boostrapServers not specified for producer");
        }
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName not specified for producer");
        }

        if (groupId.isBlank()) {
            groupId = DEFAULT_GROUP_ID;
        }

        if (maxPollRecords == null || maxPollRecords.isBlank()) {
            maxPollRecords = DEFAULT_MAX_POLL_RECORDS;
        }
        if (batchSizeBytes == null || batchSizeBytes.isBlank()) {
            batchSizeBytes = DEFAULT_BATCH_SIZE_BYTES;
        }
        if (bufferMemoryBytes == null || bufferMemoryBytes.isBlank()) {
            bufferMemoryBytes = DEFAULT_BUFFER_MEMORY_BYTES;
        }
        if (lingerMs == null || lingerMs.isBlank()) {
            lingerMs = DEFAULT_LINGER_MS;
        }
        if (requestTimeout == null || requestTimeout.isBlank()) {
            requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        }
        if (fetchMinBytes == null || fetchMinBytes.isBlank()) {
            fetchMinBytes = DEFAULT_FETCH_MIN_BYTES;
        }
        if (fetchMaxWaitMS == null || fetchMaxWaitMS.isBlank()) {
            fetchMaxWaitMS = DEFAULT_FETCH_MAX_WAIT_MS;
        }

        Consumer consumer = new Consumer();
        consumer.start(bootstrapServers, zkConnectString, topicName, groupId, Integer.parseInt(maxPollRecords),
            Integer.parseInt(batchSizeBytes), Integer.parseInt(bufferMemoryBytes), Integer.parseInt(lingerMs),
            Integer.parseInt(requestTimeout), Integer.parseInt(fetchMinBytes), Integer.parseInt(fetchMaxWaitMS), false, Integer.parseInt(port));
    }

    private static String getConfig(String configName, String configValue, Properties props) {
        if (configValue == null || configValue.isBlank()) {
            configValue = props.getProperty(configName);
        }
        return configValue;
    }

    public void shutdown() {
        if (crossDcConsumer != null) {
            crossDcConsumer.shutdown();
        }
    }

    /**
     * Abstract class for defining cross-dc consumer
     */
    public abstract static class CrossDcConsumer implements Runnable {
        SolrMessageProcessor messageProcessor;
        abstract void shutdown();

    }

}
