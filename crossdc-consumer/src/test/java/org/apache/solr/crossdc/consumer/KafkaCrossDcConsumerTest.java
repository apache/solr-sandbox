package org.apache.solr.crossdc.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.messageprocessor.SolrMessageProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaCrossDcConsumerTest {

  private KafkaCrossDcConsumer kafkaCrossDcConsumer;
  private KafkaConsumer<String,MirroredSolrRequest> kafkaConsumerMock;
  private CloudSolrClient solrClientMock;
  private KafkaMirroringSink kafkaMirroringSinkMock;

  private SolrMessageProcessor messageProcessorMock;

  @Before public void setUp() {
    kafkaConsumerMock = mock(KafkaConsumer.class);
    solrClientMock = mock(CloudSolrClient.class);
    kafkaMirroringSinkMock = mock(KafkaMirroringSink.class);
    messageProcessorMock = mock(SolrMessageProcessor.class);
    KafkaCrossDcConf conf = testCrossDCConf();
    // Set necessary configurations

    kafkaCrossDcConsumer = new KafkaCrossDcConsumer(conf, new CountDownLatch(0)) {
      @Override public KafkaConsumer<String,MirroredSolrRequest> createConsumer(Properties properties) {
        return kafkaConsumerMock;
      }

      @Override protected CloudSolrClient createSolrClient(KafkaCrossDcConf conf) {
        return solrClientMock;
      }

      @Override protected KafkaMirroringSink createKafkaMirroringSink(KafkaCrossDcConf conf) {
        return kafkaMirroringSinkMock;
      }
    };
  }

  private static KafkaCrossDcConf testCrossDCConf() {
    Map config = new HashMap<>();
    config.put(KafkaCrossDcConf.TOPIC_NAME, "topic1");
    config.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "localhost:9092");
    KafkaCrossDcConf conf = new KafkaCrossDcConf(config);
    return conf;
  }

  @After public void tearDown() {
    kafkaCrossDcConsumer.shutdown();
  }

  @Test public void testRunAndShutdown() throws Exception {
    // Define the expected behavior of the mocks and set up the test scenario
    when(kafkaConsumerMock.poll(any())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    ExecutorService consumerThreadExecutor = Executors.newSingleThreadExecutor();

    // Run the test
    consumerThreadExecutor.submit(kafkaCrossDcConsumer);

    // Run the shutdown method
    kafkaCrossDcConsumer.shutdown();

    // Verify that the consumer was subscribed with the correct topic names
    verify(kafkaConsumerMock).subscribe(anyList());

    // Verify that the appropriate methods were called on the mocks
    verify(kafkaConsumerMock).wakeup();
    verify(solrClientMock).close();

    consumerThreadExecutor.shutdown();
    consumerThreadExecutor.awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test public void testHandleFailedResubmit() throws Exception {
    // Set up the KafkaCrossDcConsumer
    KafkaCrossDcConf testConf = testCrossDCConf();
    KafkaCrossDcConsumer consumer = spy(new KafkaCrossDcConsumer(testConf, new CountDownLatch(0)));
    doNothing().when(consumer).sendBatch(any(UpdateRequest.class), any(ConsumerRecord.class), any(KafkaCrossDcConsumer.WorkUnit.class));

    // Set up the SolrMessageProcessor mock
    SolrMessageProcessor mockMessageProcessor = mock(SolrMessageProcessor.class);
    IQueueHandler.Result<MirroredSolrRequest> failedResubmitResult = new IQueueHandler.Result<>(IQueueHandler.ResultStatus.FAILED_RESUBMIT, null);
    when(mockMessageProcessor.handleItem(any(MirroredSolrRequest.class))).thenReturn(failedResubmitResult);

        // Mock the KafkaMirroringSink
    KafkaMirroringSink mockKafkaMirroringSink = mock(KafkaMirroringSink.class);
    doNothing().when(mockKafkaMirroringSink).submit(any(MirroredSolrRequest.class));
    consumer.kafkaMirroringSink = mockKafkaMirroringSink;

    // Call the method to test
    ConsumerRecord<String, MirroredSolrRequest> record = createSampleConsumerRecord();
    consumer.processResult(record, failedResubmitResult);

    // Verify that the KafkaMirroringSink.submit() method was called
    verify(consumer.kafkaMirroringSink, times(1)).submit(record.value());
  }

  private ConsumerRecord<String,MirroredSolrRequest> createSampleConsumerRecord() {
    return new ConsumerRecord<>("sample-topic", 0, 0, "key", createSampleMirroredSolrRequest());
  }

  private ConsumerRecords<String,MirroredSolrRequest> createSampleConsumerRecords() {
    TopicPartition topicPartition = new TopicPartition("sample-topic", 0);
    List<ConsumerRecord<String,MirroredSolrRequest>> recordsList = new ArrayList<>();
    recordsList.add(new ConsumerRecord<>("sample-topic", 0, 0, "key", createSampleMirroredSolrRequest()));
    return new ConsumerRecords<>(Collections.singletonMap(topicPartition, recordsList));
  }

  private MirroredSolrRequest createSampleMirroredSolrRequest() {
    // Create a sample MirroredSolrRequest for testing
    SolrInputDocument solrInputDocument = new SolrInputDocument();
    solrInputDocument.addField("id", "1");
    solrInputDocument.addField("title", "Sample title");
    solrInputDocument.addField("content", "Sample content");
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(solrInputDocument);
    return new MirroredSolrRequest(updateRequest);
  }
}

// You can add more test methods to cover other parts of the KafkaCrossDcConsumer class
