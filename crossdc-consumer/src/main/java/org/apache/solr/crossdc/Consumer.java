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

package org.apache.solr.crossdc;

import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Cross-DC Consumer main class
public class Consumer {
    private static boolean enabled = true;

    /**
     * ExecutorService to manage the cross-dc consumer threads.
     */
    private ExecutorService consumerThreadExecutor;

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private Server server;
    CrossDcConsumer crossDcConsumer;

    public void start(String[] args) {

        // TODO: use args for config
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(8090);
        server.setConnectors(new Connector[] {connector});
        crossDcConsumer = getCrossDcConsumer();

        // Start consumer thread
        consumerThreadExecutor = Executors.newSingleThreadExecutor();
        consumerThreadExecutor.submit(crossDcConsumer);

        // Register shutdown hook
        Thread shutdownHook = new Thread(() -> System.out.println("Shutting down consumers!"));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private CrossDcConsumer getCrossDcConsumer() {
        // nocommit - hardcoded conf
        KafkaCrossDcConf conf = new KafkaCrossDcConf("test-topic", true, "localhost:2181");
        return new KafkaCrossDcConsumer(conf);
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.start(args);
    }
}
