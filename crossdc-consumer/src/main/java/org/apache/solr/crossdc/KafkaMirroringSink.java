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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaMirroringSink implements RequestMirroringSink {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMirroringSink.class);

    private long lastSuccessfulEnqueueNanos;
    private KafkaCrossDcConf conf;
    private final Producer<String, MirroredSolrRequest> producer;

    public KafkaMirroringSink(final KafkaCrossDcConf conf) {
        // Create Kafka Mirroring Sink
        this.conf = conf;
        this.producer = initProducer();
        logger.info("KafkaMirroringSink has been created. Producer & Topic have been created successfully! Configurations {}", conf);
    }

    @Override
    public void submit(MirroredSolrRequest request) throws MirroringException {
        if (logger.isDebugEnabled()) {
            logger.debug("About to submit a MirroredSolrRequest");
        }

        final long enqueueStartNanos = System.nanoTime();

        // Create Producer record

        try {
            lastSuccessfulEnqueueNanos = System.nanoTime();
            // Record time since last successful enque as 0
            long elapsedTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - enqueueStartNanos);
            // Update elapsed time

            if (elapsedTimeMillis > conf.getSlowSubmitThresholdInMillis()) {
                slowSubmitAction(request, elapsedTimeMillis);
            }
        } catch (Exception e) {
            // We are intentionally catching all exceptions, the expected exception form this function is {@link MirroringException}

            String message = String.format("Unable to enqueue request %s, # of attempts %s", request, conf.getNumOfRetries());
            logger.error(message, e);

            throw new MirroringException(message, e);
        }
    }

    /**
     * Create and init the producer using {@link this#conf}
     * All producer configs are listed here
     * https://kafka.apache.org/documentation/#producerconfigs
     *
     * @return
     */
    private Producer<String, MirroredSolrRequest> initProducer() {
        // Initialize and return Kafka producer
        Properties props = new Properties();
        logger.info("Creating Kafka producer! Configurations {} ", conf.toString());
        Producer<String, MirroredSolrRequest> producer = new KafkaProducer(props);
        return producer;
    }

    private void slowSubmitAction(Object request, long elapsedTimeMillis) {
        logger.warn("Enqueuing the request to Kafka took more than {} millis. enqueueElapsedTime={}",
                conf.getSlowSubmitThresholdInMillis(),
                elapsedTimeMillis);
    }
}
