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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Class to run the consumer thread for Kafka. This also contains the implementation for retries and
 * resubmitting to the queue in case of temporary failures.
 */
public class KafkaCrossDcConsumer extends CrossDcConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCrossDcConsumer.class);

    private final KafkaConsumer<String, MirroredSolrRequest> consumer;
    private final KafkaMirroringSink kafkaMirroringSink;

    private final int KAFKA_CONSUMER_POLL_TIMEOUT_MS = 100;
    MessageProcessor messageProcessor;

    /**
     * @param conf The Kafka consumer configuration
     */
    public KafkaCrossDcConsumer(KafkaCrossDcConf conf) {
        final Properties kafkaConsumerProp = new Properties();
        logger.info("Creating Kafka consumer with configuration {}", kafkaConsumerProp);
        consumer = createConsumer(kafkaConsumerProp);

        // Create producer for resubmitting failed requests
        logger.info("Creating Kafka resubmit producer");
        this.kafkaMirroringSink = new KafkaMirroringSink(conf);
        logger.info("Created Kafka resubmit producer");

    }

    private KafkaConsumer<String, MirroredSolrRequest> createConsumer(Properties properties) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        return kafkaConsumer;
    }

    /**
     * This is where the magic happens.
     * 1. Polls and gets the packets from the queue
     * 2. Extract the MirroredSolrRequest objects
     * 3. Send the request to the MirroredSolrRequestHandler that has the processing, retry, error handling logic.
     */
    @Override
    public void run() {
        logger.info("About to start Kafka consumer thread ");
        String topic="topic";

        logger.info("Kafka consumer subscribing to topic topic={}", topic);
        consumer.subscribe(Collections.singleton(topic));

        while (pollAndProcessRequests()) {
            //no-op within this loop: everything is done in pollAndProcessRequests method defined above.
        }

        logger.info("Closed kafka consumer. Exiting now.");
        consumer.close();

    }

    /**
     * Polls and processes the requests from Kafka. This method returns false when the consumer needs to be
     * shutdown i.e. when there's a wakeup exception.
     */
    boolean pollAndProcessRequests() {
        try {
            ConsumerRecords<String, MirroredSolrRequest> records = consumer.poll(Duration.ofMillis(KAFKA_CONSUMER_POLL_TIMEOUT_MS));
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords = records.records(partition);
                try {
                    for (ConsumerRecord<String, MirroredSolrRequest> record : partitionRecords) {
                        logger.trace("Fetched record from topic={} partition={} key={} value={}",
                                record.topic(), record.partition(), record.key(), record.value());
                        IQueueHandler.Result result = messageProcessor.handleItem(record.value());
                        switch (result.status()) {
                            case FAILED_RESUBMIT:
                                kafkaMirroringSink.submit(record.value());
                                break;
                            case HANDLED:
                                // no-op
                                break;
                            case NOT_HANDLED_SHUTDOWN:
                            case FAILED_RETRY:
                                logger.error("Unexpected response while processing request. We never expect {}.",
                                        result.status().toString());
                                break;
                            default:
                                // no-op
                        }
                    }
                    updateOffset(partition, partitionRecords);

                    // handleItem sets the thread interrupt, let's exit if there has been an interrupt set
                    if(Thread.currentThread().isInterrupted()) {
                        logger.info("Kafka Consumer thread interrupted, shutting down Kafka consumer.");
                        return false;
                    }
                } catch (MirroringException e) {
                    // We don't really know what to do here, so it's wiser to just break out.
                    logger.error("Mirroring exception occured while resubmitting to Kafka. We are going to stop the consumer thread now.", e);
                    return false;
                } catch (WakeupException e) {
                    logger.info("Caught wakeup exception, shutting down KafkaSolrRequestConsumer.");
                    return false;
                } catch (Exception e) {
                    // If there is any exception returned by handleItem, then reset the offset.
                    logger.warn("Exception occurred in Kafka consumer thread, but we will continue.", e);
                    resetOffsetForPartition(partition, partitionRecords);
                    break;
                }
            }
        } catch (WakeupException e) {
            logger.info("Caught wakeup exception, shutting down KafkaSolrRequestConsumer");
            return false;
        } catch (Exception e) {
            logger.error("Exception occurred in Kafka consumer thread, but we will continue.", e);
        }
        return true;
    }

    /**
     * Reset the local offset so that the consumer reads the records from Kafka again.
     * @param partition The TopicPartition to reset the offset for
     * @param partitionRecords PartitionRecords for the specified partition
     */
    private void resetOffsetForPartition(TopicPartition partition, List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords) {
        logger.debug("Resetting offset to: {}", partitionRecords.get(0).offset());
        long resetOffset = partitionRecords.get(0).offset();
        consumer.seek(partition, resetOffset);
    }

    /**
     * Logs and updates the commit point for the partition that has been processed.
     * @param partition The TopicPartition to update the offset for
     * @param partitionRecords PartitionRecords for the specified partition
     */
    private void updateOffset(TopicPartition partition, List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords) {
        long nextOffset = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(nextOffset)));

        logger.trace("Updated offset for topic={} partition={} to offset={}",
                partition.topic(), partition.partition(), nextOffset);
    }

    /**
     * Shutdown the Kafka consumer by calling wakeup.
     */
    void shutdown() {
        consumer.wakeup();
    }


}
