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
import org.apache.solr.crossdc.common.ConfigProperty;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.*;

// Cross-DC Consumer main class
public class Consumer {




    private static boolean enabled = true;

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * ExecutorService to manage the cross-dc consumer threads.
     */
    private ExecutorService consumerThreadExecutor;

    private Server server;
    CrossDcConsumer crossDcConsumer;

    public void start(Map<String, String> properties) {


        //server = new Server();
        //ServerConnector connector = new ServerConnector(server);
        //connector.setPort(port);
        //server.setConnectors(new Connector[] {connector})
        KafkaCrossDcConf conf = new KafkaCrossDcConf(properties);
        crossDcConsumer = getCrossDcConsumer(conf);

        // Start consumer thread

        log.info("Starting CrossDC Consumer {}", conf);

        consumerThreadExecutor = Executors.newSingleThreadExecutor();
        consumerThreadExecutor.submit(crossDcConsumer);

        // Register shutdown hook
        Thread shutdownHook = new Thread(() -> System.out.println("Shutting down consumers!"));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private CrossDcConsumer getCrossDcConsumer(KafkaCrossDcConf conf) {
        return new KafkaCrossDcConsumer(conf);
    }

    public static void main(String[] args) {

        Map<String,String> properties = new HashMap<>();

        for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
            properties.put(configKey.getKey(), System.getProperty(configKey.getKey()));
        }

        String zkConnectString = properties.get(KafkaCrossDcConf.ZK_CONNECT_STRING);
        if (zkConnectString == null || zkConnectString.isBlank()) {
            throw new IllegalArgumentException("zkConnectString not specified for Consumer");
        }

        try (SolrZkClient client = new SolrZkClient(zkConnectString, 15000)) {

            try {
                if (client.exists(System.getProperty(CrossDcConf.ZK_CROSSDC_PROPS_PATH, KafkaCrossDcConf.CROSSDC_PROPERTIES), true)) {
                    byte[] data = client.getData(System.getProperty(CrossDcConf.ZK_CROSSDC_PROPS_PATH, KafkaCrossDcConf.CROSSDC_PROPERTIES), null, null, true);
                    Properties zkProps = new Properties();
                    zkProps.load(new ByteArrayInputStream(data));
                    Properties zkPropsUnproccessed = new Properties(zkProps);

                    for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
                        if (properties.get(configKey.getKey()) == null || properties.get(configKey.getKey()).isBlank()) {
                            properties.put(configKey.getKey(), (String) zkProps.get(configKey.getKey()));
                            zkPropsUnproccessed.remove(configKey.getKey());
                        }
                    }
                    zkPropsUnproccessed.forEach((k, v) -> {
                        properties.put((String) k, (String) v);
                    });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
            } catch (Exception e) {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            }
        }

        String bootstrapServers = properties.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers not specified for Consumer");
        }

        String topicName = properties.get(TOPIC_NAME);
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName not specified for Consumer");
        }

        Consumer consumer = new Consumer();
        consumer.start(properties);
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
