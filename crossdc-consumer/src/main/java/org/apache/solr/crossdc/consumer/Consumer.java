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

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.crossdc.common.ConfUtil;
import org.apache.solr.crossdc.common.ConfigProperty;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.SensitivePropRedactionUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.*;

// Cross-DC Consumer main class
public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String METRICS_REGISTRY = "metrics";

    private Server server;
    private CrossDcConsumer crossDcConsumer;

    private CountDownLatch startLatch = new CountDownLatch(1);


    public void start() {
        start(new HashMap<>());
    }

    public void start(Map<String,Object> properties ) {

        ConfUtil.fillProperties(null, properties);

        log.info("Consumer startup config properties before adding additional properties from Zookeeper={}",
                SensitivePropRedactionUtils.flattenAndRedactForLogging(properties));

        String zkConnectString = (String) properties.get("zkConnectString");
        if (zkConnectString == null) {
            throw new IllegalArgumentException("zkConnectString not specified for producer");
        }

        try (SolrZkClient client = new SolrZkClient.Builder().withUrl(zkConnectString).withTimeout(15000, TimeUnit.MILLISECONDS).build()) {
            // update properties, potentially also from ZK
            ConfUtil.fillProperties(client, properties);
        }

        ConfUtil.verifyProperties(properties);

        String bootstrapServers = (String) properties.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS);
        String topicName = (String) properties.get(TOPIC_NAME);

        KafkaCrossDcConf conf = new KafkaCrossDcConf(properties);
        crossDcConsumer = getCrossDcConsumer(conf, startLatch);

        // jetty endpoint for /metrics
        int port = conf.getInt(PORT);
        if (port > 0) {
            log.info("Starting API endpoints...");
            server = new Server(port);
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            context.setContextPath("/");
            server.setHandler(context);
            context.addServlet(ThreadDumpServlet.class, "/threads/*");
            context.addServlet(MetricsServlet.class, "/metrics/*");
            context.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry", SharedMetricRegistries.getOrCreate(METRICS_REGISTRY));
            for (ServletMapping mapping : context.getServletHandler().getServletMappings()) {
                log.info(" - {}", mapping.getPathSpecs()[0]);
            }
        }

        // Start consumer thread

        log.info("Starting CrossDC Consumer {}", conf);

        ExecutorService consumerThreadExecutor = Executors.newSingleThreadExecutor();
        consumerThreadExecutor.submit(crossDcConsumer);

        // Register shutdown hook
        Thread shutdownHook = new Thread(() -> System.out.println("Shutting down consumers!"));
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        if (server != null) {
            try {
                server.start();
            } catch (Exception e) {
                throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
            }
        }
        try {
            startLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e1) {
                    // ignore
                }
            }
            throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
        }
    }

    protected CrossDcConsumer getCrossDcConsumer(KafkaCrossDcConf conf, CountDownLatch startLatch) {
        return new KafkaCrossDcConsumer(conf, startLatch);
    }

    public static void main(String[] args) {

        Consumer consumer = new Consumer();
        consumer.start();
    }

    public final void shutdown() {
        if (crossDcConsumer != null) {
            crossDcConsumer.shutdown();
        }
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Abstract class for defining cross-dc consumer
     */
    public abstract static class CrossDcConsumer implements Runnable {
        abstract void shutdown();

    }
}
