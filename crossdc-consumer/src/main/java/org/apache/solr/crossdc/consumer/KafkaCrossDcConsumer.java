package org.apache.solr.crossdc.consumer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
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

  private final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("metrics");

  private final KafkaConsumer<String, MirroredSolrRequest> consumer;
  private final KafkaMirroringSink kafkaMirroringSink;

  private final static int KAFKA_CONSUMER_POLL_TIMEOUT_MS = 5000;
  private final String topicName;
  private final SolrMessageProcessor messageProcessor;

  private final CloudSolrClient solrClient;

  /**
   * @param conf The Kafka consumer configuration
   */
  public KafkaCrossDcConsumer(KafkaCrossDcConf conf) {
    this.topicName = conf.get(KafkaCrossDcConf.TOPIC_NAME);

    final Properties kafkaConsumerProps = new Properties();

    kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));

    kafkaConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, conf.get(KafkaCrossDcConf.GROUP_ID));

    kafkaConsumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_POLL_RECORDS));

    kafkaConsumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, conf.get(KafkaCrossDcConf.MAX_POLL_INTERVAL_MS));

    kafkaConsumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, conf.get(KafkaCrossDcConf.SESSION_TIMEOUT_MS));
    kafkaConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    kafkaConsumerProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MIN_BYTES));
    kafkaConsumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_WAIT_MS));

    kafkaConsumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_BYTES));
    kafkaConsumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_PARTITION_FETCH_BYTES));

    KafkaCrossDcConf.addSecurityProps(conf, kafkaConsumerProps);

    kafkaConsumerProps.putAll(conf.getAdditionalProperties());

    solrClient =
        new CloudSolrClient.Builder(Collections.singletonList(conf.get(KafkaCrossDcConf.ZK_CONNECT_STRING)),
            Optional.empty()).build();

    messageProcessor = new SolrMessageProcessor(solrClient, resubmitRequest -> 0L);

    log.info("Creating Kafka consumer with configuration {}", kafkaConsumerProps);
    consumer = createConsumer(kafkaConsumerProps);

    // Create producer for resubmitting failed requests
    log.info("Creating Kafka resubmit producer");
    this.kafkaMirroringSink = new KafkaMirroringSink(conf);
    log.info("Created Kafka resubmit producer");

  }

  public static KafkaConsumer<String, MirroredSolrRequest> createConsumer(Properties properties) {
    return new KafkaConsumer<>(properties, new StringDeserializer(),
        new MirroredSolrRequestSerializer());
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
            IQueueHandler.Result<MirroredSolrRequest> result = messageProcessor.handleItem(record.value());
            if (log.isTraceEnabled()) {
              log.trace("Finished processing record from topic={} partition={} key={} value={} result={}",
                  record.topic(), record.partition(), record.key(), record.value(), result);
            }

            switch (result.status()) {
              case FAILED_RESUBMIT:
                // currently, we use a strategy taken from an earlier working implementation
                // of just resubmitting back to the queue - note that in rare cases, this could
                // allow for incorrect update reorders

                log.info("Resubmitting failed Solr update to Kafka queue");

                metrics.counter("failed-resubmit").inc();
                kafkaMirroringSink.submit(record.value());
                break;
              case HANDLED:
                // no-op
                if (log.isTraceEnabled()) {
                  log.trace("result=handled");
                }
                metrics.counter("handled").inc();
                break;
              case NOT_HANDLED_SHUTDOWN:
                if (log.isTraceEnabled()) {
                  log.trace("result=nothandled_shutdown");
                }
                metrics.counter("nothandled_shutdown").inc();
              case FAILED_RETRY:
                log.error("Unexpected response while processing request. We never expect {}.",
                    result.status().toString());
                metrics.counter("failed-retry").inc();
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

          if (e instanceof ClassCastException || e instanceof SerializationException) { // TODO: optional
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

      if (e instanceof ClassCastException || e instanceof SerializationException) { // TODO: optional
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
    log.info("Resetting offset to: {}", partitionRecords.get(0).offset());
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

    if (log.isDebugEnabled()) {
      log.trace("Updating offset for topic={} partition={} to offset={}", partition.topic(),
          partition.partition(), nextOffset);
    }

    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(nextOffset)));

    if (log.isDebugEnabled()) {
      log.trace("Finished updating offset for topic={} partition={} to offset={}", partition.topic(),
          partition.partition(), nextOffset);
    }
  }

  /**
   * Shutdown the Kafka consumer by calling wakeup.
   */
  public final void shutdown() {
    log.info("Shutdown called on KafkaCrossDcConsumer");
    try {
      solrClient.close();
    } catch (Exception e) {
      log.warn("Exception closing Solr client on shutdown");
    }
    consumer.wakeup();
  }

}
