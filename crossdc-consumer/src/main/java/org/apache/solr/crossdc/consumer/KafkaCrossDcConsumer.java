package org.apache.solr.crossdc.consumer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.crossdc.common.*;
import org.apache.solr.crossdc.messageprocessor.SolrMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * This is a Java class called KafkaCrossDcConsumer, which is part of the Apache Solr framework.
 * It consumes messages from Kafka and mirrors them into a Solr instance. It uses a KafkaConsumer
 * object to subscribe to one or more topics and receive ConsumerRecords that contain MirroredSolrRequest
 * objects. The SolrMessageProcessor handles each MirroredSolrRequest and sends the resulting
 * UpdateRequest to the CloudSolrClient for indexing. A ThreadPoolExecutor is used to handle the update
 * requests asynchronously. The KafkaCrossDcConsumer also handles offset management, committing offsets
 * to Kafka and can seek to specific offsets for error recovery. The class provides methods to start and
 * top the consumer thread.
 */
public class KafkaCrossDcConsumer extends Consumer.CrossDcConsumer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("metrics");

  private final KafkaConsumer<String,MirroredSolrRequest> consumer;
  private final CountDownLatch startLatch;
  KafkaMirroringSink kafkaMirroringSink;

  private final static int KAFKA_CONSUMER_POLL_TIMEOUT_MS = 5000;
  private final String[] topicNames;
  private final SolrMessageProcessor messageProcessor;

  private final CloudSolrClient solrClient;

  private final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(10) {
    @Override public boolean offer(Runnable r) {
      //return super.offer(r);
      try {
        super.put(r);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      return true;
    }

  };
  private final ThreadPoolExecutor executor;

  private final ConcurrentHashMap<TopicPartition,PartitionWork> partitionWorkMap = new ConcurrentHashMap<>();

  /**
   * @param conf       The Kafka consumer configuration
   * @param startLatch
   */
  public KafkaCrossDcConsumer(KafkaCrossDcConf conf, CountDownLatch startLatch) {

    this.topicNames = conf.get(KafkaCrossDcConf.TOPIC_NAME).split(",");
    this.startLatch = startLatch;
    final Properties kafkaConsumerProps = new Properties();

    kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));

    kafkaConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, conf.get(KafkaCrossDcConf.GROUP_ID));

    kafkaConsumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_POLL_RECORDS));

    kafkaConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    kafkaConsumerProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MIN_BYTES));
    kafkaConsumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_WAIT_MS));

    kafkaConsumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_BYTES));
    kafkaConsumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_PARTITION_FETCH_BYTES));

    KafkaCrossDcConf.addSecurityProps(conf, kafkaConsumerProps);

    kafkaConsumerProps.putAll(conf.getAdditionalProperties());
    int threads = conf.getInt(KafkaCrossDcConf.CONSUMER_PROCESSING_THREADS);
    executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, queue, new ThreadFactory() {
      @Override public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("KafkaCrossDcConsumerWorker");
        return t;
      }
    });
    executor.prestartAllCoreThreads();

    solrClient = createSolrClient(conf);

    messageProcessor = new SolrMessageProcessor(solrClient, resubmitRequest -> 0L);

    log.info("Creating Kafka consumer with configuration {}", kafkaConsumerProps);
    consumer = createConsumer(kafkaConsumerProps);

    // Create producer for resubmitting failed requests
    log.info("Creating Kafka resubmit producer");
    this.kafkaMirroringSink = createKafkaMirroringSink(conf);
    log.info("Created Kafka resubmit producer");

  }

  public KafkaConsumer<String,MirroredSolrRequest> createConsumer(Properties properties) {
    return new KafkaConsumer<>(properties, new StringDeserializer(), new MirroredSolrRequestSerializer());
  }

  /**
   * This is where the magic happens.
   * 1. Polls and gets the packets from the queue
   * 2. Extract the MirroredSolrRequest objects
   * 3. Send the request to the MirroredSolrRequestHandler that has the processing, retry, error handling logic.
   */
  @Override public void run() {
    log.info("About to start Kafka consumer thread, topics={}", Arrays.asList(topicNames));

    try {

      consumer.subscribe(Arrays.asList((topicNames)));

      log.info("Consumer started");
      startLatch.countDown();

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
      for (TopicPartition partition : partitionWorkMap.keySet()) {
        checkForOffsetUpdates(partition);
      }

      ConsumerRecords<String,MirroredSolrRequest> records = consumer.poll(Duration.ofMillis(KAFKA_CONSUMER_POLL_TIMEOUT_MS));

      UpdateRequest solrReqBatch = null;

      ConsumerRecord<String,MirroredSolrRequest> lastRecord = null;

      for (TopicPartition partition : records.partitions()) {
        List<ConsumerRecord<String,MirroredSolrRequest>> partitionRecords = records.records(partition);

        PartitionWork partitionWork = partitionWorkMap.compute(partition, (k, v) -> {
          if (v == null) {
            return new PartitionWork();
          }
          return v;
        });
        WorkUnit workUnit = new WorkUnit();
        workUnit.nextOffset = getOffsetForPartition(partitionRecords);
        partitionWork.partitionQueue.add(workUnit);
        try {
          ModifiableSolrParams lastParams = null;
          NamedList lastParamsAsNamedList = null;
          solrReqBatch = new UpdateRequest();
          for (ConsumerRecord<String,MirroredSolrRequest> requestRecord : partitionRecords) {
            if (log.isTraceEnabled()) {
              log.trace("Fetched record from topic={} partition={} key={} value={}", requestRecord.topic(), requestRecord.partition(), requestRecord.key(),
                  requestRecord.value());
            }

            lastRecord = requestRecord;
            MirroredSolrRequest req = requestRecord.value();
            UpdateRequest solrReq = (UpdateRequest) req.getSolrRequest();
            ModifiableSolrParams params = solrReq.getParams();
            if (log.isTraceEnabled()) {
              log.trace("params={}", params);
            }

            if (lastParams != null && !lastParams.toNamedList().equals(params.toNamedList())) {
              if (log.isTraceEnabled()) {
                log.trace("SolrParams have changed, starting new UpdateRequest, params={}", params);
              }
              lastParamsAsNamedList = null;
              sendBatch(solrReqBatch, lastRecord, workUnit);
              solrReqBatch = new UpdateRequest();
              workUnit = new WorkUnit();
              workUnit.nextOffset = getOffsetForPartition(partitionRecords);
              partitionWork.partitionQueue.add(workUnit);
            }

            lastParams = solrReq.getParams();
            solrReqBatch.setParams(params);
            if (lastParamsAsNamedList == null) {
              lastParamsAsNamedList = lastParams.toNamedList();
            }

            List<SolrInputDocument> docs = solrReq.getDocuments();
            if (docs != null) {
              solrReqBatch.add(docs);
            }
            List<String> deletes = solrReq.getDeleteById();
            if (deletes != null) {
              solrReqBatch.deleteById(deletes);
            }
            List<String> deleteByQuery = solrReq.getDeleteQuery();
            if (deleteByQuery != null) {
              for (String delByQuery : deleteByQuery) {
                solrReqBatch.deleteByQuery(delByQuery);
              }
            }

          }

          sendBatch(solrReqBatch, lastRecord, workUnit);

          checkForOffsetUpdates(partition);

          // handleItem sets the thread interrupt, let's exit if there has been an interrupt set
          if (Thread.currentThread().isInterrupted()) {
            log.info("Kafka Consumer thread interrupted, shutting down Kafka consumer.");
            return false;
          }
        } catch (WakeupException e) {
          log.info("Caught wakeup exception, shutting down KafkaSolrRequestConsumer.");
          return false;
        } catch (Exception e) {
          // If there is any exception returned by handleItem, don't set the offset.

          if (e instanceof ClassCastException || e instanceof SerializationException) { // TODO: optional
            log.error("Non retryable error", e);
            break;
          }
          log.error("Exception occurred in Kafka consumer thread, stopping the Consumer.", e);
          break;
        }
      }

      for (TopicPartition partition : partitionWorkMap.keySet()) {
        checkForOffsetUpdates(partition);
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

  public void sendBatch(UpdateRequest solrReqBatch, ConsumerRecord<String,MirroredSolrRequest> lastRecord, WorkUnit workUnit) {
    UpdateRequest finalSolrReqBatch = solrReqBatch;
    Future<?> future = executor.submit(() -> {
      IQueueHandler.Result<MirroredSolrRequest> result = messageProcessor.handleItem(new MirroredSolrRequest(finalSolrReqBatch));
      try {
        processResult(lastRecord, result);
      } catch (MirroringException e) {
        // We don't really know what to do here
        log.error("Mirroring exception occurred while resubmitting to Kafka. We are going to stop the consumer thread now.", e);
        throw new RuntimeException(e);
      }
    });
    workUnit.workItems.add(future);
  }

  private void checkForOffsetUpdates(TopicPartition partition) {
    PartitionWork work;
    while ((work = partitionWorkMap.get(partition)) != null) {
      WorkUnit workUnit = work.partitionQueue.peek();
      if (workUnit != null) {
        for (Future<?> future : workUnit.workItems) {
          if (!future.isDone()) {
            if (log.isTraceEnabled()) {
              log.trace("Future for update is not done topic={}", partition.topic());
            }
            return;
          }
          if (log.isTraceEnabled()) {
            log.trace("Future for update is done topic={}", partition.topic());
          }
          try {
            future.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            log.error("Exception resubmitting updates to Kafka, stopping the Consumer thread", e);
            work.partitionQueue.poll();
            throw new RuntimeException("Exception resubmitting updates to Kafka, stopping the Consumer thread", e);
          }
          work.partitionQueue.poll();
        }

        updateOffset(partition, workUnit.nextOffset);

      }
    }
  }

  void processResult(ConsumerRecord<String,MirroredSolrRequest> record, IQueueHandler.Result<MirroredSolrRequest> result) throws MirroringException {
    switch (result.status()) {
      case FAILED_RESUBMIT:
        // currently, we use a strategy taken from an earlier working implementation
        // of just resubmitting back to the queue - note that in rare cases, this could
        // allow for incorrect update reorders
        if (log.isTraceEnabled()) {
          log.trace("result=failed-resubmit");
        }
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
        log.error("Unexpected response while processing request. We never expect {}.", result.status().toString());
        metrics.counter("failed-retry").inc();
        break;
      default:
        if (log.isTraceEnabled()) {
          log.trace("result=no matching case");
        }
        // no-op
    }
  }

  /**
   * Reset the local offset so that the consumer reads the records from Kafka again.
   *
   * @param partition        The TopicPartition to reset the offset for
   * @param partitionRecords PartitionRecords for the specified partition
   */
  private void resetOffsetForPartition(TopicPartition partition, List<ConsumerRecord<String,MirroredSolrRequest>> partitionRecords) {
    if (log.isTraceEnabled()) {
      log.trace("Resetting offset to: {}", partitionRecords.get(0).offset());
    }
    long resetOffset = partitionRecords.get(0).offset();
    consumer.seek(partition, resetOffset);
  }

  /**
   * Logs and updates the commit point for the partition that has been processed.
   *
   * @param partition  The TopicPartition to update the offset for
   * @param nextOffset The next offset to commit for this partition.
   */
  private void updateOffset(TopicPartition partition, long nextOffset) {
    if (log.isTraceEnabled()) {
      log.trace("Updated offset for topic={} partition={} to offset={}", partition.topic(), partition.partition(), nextOffset);
    }

    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(nextOffset)));
  }

  private static long getOffsetForPartition(List<ConsumerRecord<String,MirroredSolrRequest>> partitionRecords) {
    long nextOffset = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
    return nextOffset;
  }

  /**
   * Shutdown the Kafka consumer by calling wakeup.
   */
  public final void shutdown() {
    consumer.wakeup();
    log.info("Shutdown called on KafkaCrossDcConsumer");
    try {
      if (!executor.isShutdown()) {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
      solrClient.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted while waiting for executor to shutdown");
    } catch (Exception e) {
      log.warn("Exception closing Solr client on shutdown", e);
    }
  }

  protected CloudSolrClient createSolrClient(KafkaCrossDcConf conf) {
    return new CloudSolrClient.Builder(Collections.singletonList(conf.get(KafkaCrossDcConf.ZK_CONNECT_STRING)), Optional.empty()).build();
  }

  protected KafkaMirroringSink createKafkaMirroringSink(KafkaCrossDcConf conf) {
    return new KafkaMirroringSink(conf);
  }

  private class PartitionWork {
    private final Queue<WorkUnit> partitionQueue = new LinkedList<>();
  }

  static class WorkUnit {
    Set<Future<?>> workItems = new HashSet<>();
    long nextOffset;
  }
}
