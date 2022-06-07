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

// Cross-DC Consumer main class
public class Consumer {
    public static final String DEFAULT_PORT = "8090";
    private static boolean enabled = true;

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * ExecutorService to manage the cross-dc consumer threads.
     */
    private ExecutorService consumerThreadExecutor;

    private Server server;
    CrossDcConsumer crossDcConsumer;
    private String topicName;

    public void start(String bootstrapServers, String zkConnectString, String topicName, boolean enableDataEncryption, int port) {
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrapServers config was not passed at startup");
        }
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("zkConnectString config was not passed at startup");
        }
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("topicName config was not passed at startup");
        }

        this.topicName = topicName;

        //server = new Server();
        //ServerConnector connector = new ServerConnector(server);
        //connector.setPort(port);
        //server.setConnectors(new Connector[] {connector})

        crossDcConsumer = getCrossDcConsumer(bootstrapServers, zkConnectString, topicName, enableDataEncryption);

        // Start consumer thread

        log.info("Starting CrossDC Consumer bootstrapServers={}, zkConnectString={}, topicName={}, enableDataEncryption={}", bootstrapServers, zkConnectString, topicName, enableDataEncryption);

        consumerThreadExecutor = Executors.newSingleThreadExecutor();
        consumerThreadExecutor.submit(crossDcConsumer);

        // Register shutdown hook
        Thread shutdownHook = new Thread(() -> System.out.println("Shutting down consumers!"));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private CrossDcConsumer getCrossDcConsumer(String bootstrapServers, String zkConnectString, String topicName,
        boolean enableDataEncryption) {

        KafkaCrossDcConf conf = new KafkaCrossDcConf(bootstrapServers, topicName, enableDataEncryption, zkConnectString);
        return new KafkaCrossDcConsumer(conf);
    }

    public static void main(String[] args) {

        String zkConnectString = System.getProperty("zkConnectString");
        if (zkConnectString == null || zkConnectString.isBlank()) {
            throw new IllegalArgumentException("zkConnectString not specified for producer");
        }

        String bootstrapServers = System.getProperty("bootstrapServers");
        // boolean enableDataEncryption = Boolean.getBoolean("enableDataEncryption");
        String topicName = System.getProperty("topicName");
        String port = System.getProperty("port");


        try (SolrZkClient client = new SolrZkClient(zkConnectString, 15000)) {

            try {
                if ((topicName == null || topicName.isBlank())
                    || (bootstrapServers == null || bootstrapServers.isBlank()) || (port == null || port.isBlank()) && client
                    .exists(CrossDcConf.CROSSDC_PROPERTIES, true)) {
                    byte[] data = client.getData("/crossdc.properties", null, null, true);
                    Properties props = new Properties();
                    props.load(new ByteArrayInputStream(data));

                    if (topicName == null || topicName.isBlank()) {
                        topicName = props.getProperty("topicName");
                    }
                    if (bootstrapServers == null || bootstrapServers.isBlank()) {
                        bootstrapServers = props.getProperty("bootstrapServers");
                    }
                    if (port == null || port.isBlank()) {
                        port = props.getProperty("port");
                    }
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

        Consumer consumer = new Consumer();
        consumer.start(bootstrapServers, zkConnectString, topicName, false, Integer.parseInt(port));
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
