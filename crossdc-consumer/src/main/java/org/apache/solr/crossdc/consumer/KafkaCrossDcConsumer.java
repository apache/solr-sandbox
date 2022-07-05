package org.apache.solr.crossdc.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.crossdc.common.*;
import org.apache.solr.crossdc.messageprocessor.SolrMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Class to run the consumer thread for Kafka. This also contains the implementation for retries and
 * resubmitting to the queue in case of temporary failures.
 */
public class KafkaCrossDcConsumer extends Consumer.CrossDcConsumer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final KafkaConsumer<String, MirroredSolrRequest> consumer;
  private final KafkaMirroringSink kafkaMirroringSink;

  private final int KAFKA_CONSUMER_POLL_TIMEOUT_MS = 5000;
  private final String topicName;
  SolrMessageProcessor messageProcessor;

  CloudSolrClient solrClient;

  /**
   * @param conf The Kafka consumer configuration
   */
  public KafkaCrossDcConsumer(KafkaCrossDcConf conf) {
    this.topicName = conf.getTopicName();

    final Properties kafkaConsumerProp = new Properties();

    kafkaConsumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBootStrapServers());

    kafkaConsumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, "group_1"); // TODO

    kafkaConsumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    solrClient =
        new CloudSolrClient.Builder(Collections.singletonList(conf.getSolrZkConnectString()),
            Optional.empty()).build();

    messageProcessor = new SolrMessageProcessor(solrClient, new ResubmitBackoffPolicy() {
      @Override public long getBackoffTimeMs(MirroredSolrRequest resubmitRequest) {
        return 0;
      }
    });

    log.info("Creating Kafka consumer with configuration {}", kafkaConsumerProp);
    consumer = createConsumer(kafkaConsumerProp);

    // Create producer for resubmitting failed requests
    log.info("Creating Kafka resubmit producer");
    this.kafkaMirroringSink = new KafkaMirroringSink(conf);
    log.info("Created Kafka resubmit producer");

  }

  private KafkaConsumer<String, MirroredSolrRequest> createConsumer(Properties properties) {
    KafkaConsumer kafkaConsumer = new KafkaConsumer(properties, new StringDeserializer(),
        new MirroredSolrRequestSerializer());
    return kafkaConsumer;
  }

  /**
   * This is where the magic happens.
   * 1. Polls and gets the packets from the queue
   * 2. Extract the MirroredSolrRequest objects
   * 3. Send the request to the MirroredSolrRequestHandler that has the processing, retry, error handling logic.
   */
  @Override public void run() {
    log.info("About to start Kafka consumer thread, topic={}", topicName);

    try {

      consumer.subscribe(Collections.singleton(topicName));

      while (pollAndProcessRequests()) {
        //no-op within this loop: everything is done in pollAndProcessRequests method defined above.
      }

      log.info("Closed kafka consumer. Exiting now.");
      try {
        consumer.close();
      } catch (Exception e) {
        log.warn("Failed to close kafka consumer", e);
      }

      try {
        kafkaMirroringSink.close();
      } catch (Exception e) {
        log.warn("Failed to close kafka mirroring sink", e);
      }
    } finally {
      IOUtils.closeQuietly(solrClient);
    }

  }

  /**
   * Polls and processes the requests from Kafka. This method returns false when the consumer needs to be
   * shutdown i.e. when there's a wakeup exception.
   */
  boolean pollAndProcessRequests() {
    log.trace("Entered pollAndProcessRequests loop");
    try {
      ConsumerRecords<String, MirroredSolrRequest> records =
          consumer.poll(Duration.ofMillis(KAFKA_CONSUMER_POLL_TIMEOUT_MS));
      for (TopicPartition partition : records.partitions()) {
        List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords =
            records.records(partition);
        try {
          for (ConsumerRecord<String, MirroredSolrRequest> record : partitionRecords) {
            if (log.isTraceEnabled()) {
              log.trace("Fetched record from topic={} partition={} key={} value={}", record.topic(),
                  record.partition(), record.key(), record.value());
            }
            IQueueHandler.Result result = messageProcessor.handleItem(record.value());
            switch (result.status()) {
              case FAILED_RESUBMIT:
                if (log.isTraceEnabled()) {
                  log.trace("result=failed-resubmit");
                }
                kafkaMirroringSink.submit(record.value());
                break;
              case HANDLED:
                // no-op
                if (log.isTraceEnabled()) {
                  log.trace("result=handled");
                }
                break;
              case NOT_HANDLED_SHUTDOWN:
                if (log.isTraceEnabled()) {
                  log.trace("result=nothandled_shutdown");
                }
              case FAILED_RETRY:
                log.error("Unexpected response while processing request. We never expect {}.",
                    result.status().toString());
                break;
              default:
                if (log.isTraceEnabled()) {
                  log.trace("result=no matching case");
                }
                // no-op
            }
          }
          updateOffset(partition, partitionRecords);

          // handleItem sets the thread interrupt, let's exit if there has been an interrupt set
          if (Thread.currentThread().isInterrupted()) {
            log.info("Kafka Consumer thread interrupted, shutting down Kafka consumer.");
            return false;
          }
        } catch (MirroringException e) {
          // We don't really know what to do here, so it's wiser to just break out.
          log.error(
              "Mirroring exception occurred while resubmitting to Kafka. We are going to stop the consumer thread now.",
              e);
          return false;
        } catch (WakeupException e) {
          log.info("Caught wakeup exception, shutting down KafkaSolrRequestConsumer.");
          return false;
        } catch (Exception e) {
          // If there is any exception returned by handleItem, then reset the offset.

          if (e instanceof ClassCastException || e instanceof ClassNotFoundException
              || e instanceof SerializationException) { // TODO: optional
            log.error("Non retryable error", e);
            break;
          }
          log.warn("Exception occurred in Kafka consumer thread, but we will continue.", e);
          resetOffsetForPartition(partition, partitionRecords);
          break;
        }
      }
    } catch (WakeupException e) {
      log.info("Caught wakeup exception, shutting down KafkaSolrRequestConsumer");
      return false;
    } catch (Exception e) {

      if (e instanceof ClassCastException || e instanceof ClassNotFoundException
          || e instanceof SerializationException) { // TODO: optional
        log.error("Non retryable error", e);
        return false;
      }

      log.error("Exception occurred in Kafka consumer thread, but we will continue.", e);
    }
    return true;
  }

  /**
   * Reset the local offset so that the consumer reads the records from Kafka again.
   *
   * @param partition        The TopicPartition to reset the offset for
   * @param partitionRecords PartitionRecords for the specified partition
   */
  private void resetOffsetForPartition(TopicPartition partition,
      List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords) {
    if (log.isTraceEnabled()) {
      log.trace("Resetting offset to: {}", partitionRecords.get(0).offset());
    }
    long resetOffset = partitionRecords.get(0).offset();
    consumer.seek(partition, resetOffset);
  }

  /**
   * Logs and updates the commit point for the partition that has been processed.
   *
   * @param partition        The TopicPartition to update the offset for
   * @param partitionRecords PartitionRecords for the specified partition
   */
  private void updateOffset(TopicPartition partition,
      List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords) {
    long nextOffset = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;

    if (log.isTraceEnabled()) {
      log.trace("Updated offset for topic={} partition={} to offset={}", partition.topic(),
          partition.partition(), nextOffset);
    }

    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(nextOffset)));
  }

  /**
   * Shutdown the Kafka consumer by calling wakeup.
   */
  public void shutdown() {
    log.info("Shutdown called on KafkaCrossDcConsumer");
    try {
      solrClient.close();
    } catch (Exception e) {
      log.warn("Exception closing Solr client on shutdown");
    }
    consumer.wakeup();
  }

}
