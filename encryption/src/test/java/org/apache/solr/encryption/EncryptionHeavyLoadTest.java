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
package org.apache.solr.encryption;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.solr.encryption.EncryptionRequestHandler.NO_KEY_ID;
import static org.apache.solr.encryption.EncryptionTestUtil.EncryptionStatus;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_2;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_3;

/**
 * Tests the encryption handler under heavy concurrent load test.
 * <p>
 * Sends concurrent indexing and querying requests with high throughput while
 * triggering re-encryption with the handler to verify concurrent segment merging
 * is handled correctly without stopping indexing nor querying, and all encrypted
 * files are decrypted correctly when refreshing the index searcher after each
 * commit.
 */
public class EncryptionHeavyLoadTest extends SolrCloudTestCase {

  // Change the test duration manually to run longer, e.g. 20 minutes.
  private static final long TEST_DURATION_NS = TimeUnit.SECONDS.toNanos(10);
  private static final int RANDOM_DELAY_BETWEEN_INDEXING_BATCHES_MS = 50;
  private static final int RANDOM_NUM_DOCS_PER_BATCH = 200;
  private static final float PROBABILITY_OF_COMMIT_PER_BATCH = 0.33f;
  private static final int DICTIONARY_SIZE = 5000;
  private static final int RANDOM_DELAY_BETWEEN_QUERIES_MS = 10;
  private static final int NUM_INDEXING_THREADS = 3;
  private static final int NUM_QUERYING_THREADS = 2;
  private static final int RANDOM_DELAY_BETWEEN_REENCRYPTION_MS = 2000;
  private static final String[] KEY_IDS = {KEY_ID_1, KEY_ID_2, KEY_ID_3, NO_KEY_ID};
  private static final float PROBABILITY_OF_WAITING_ENCRYPTION_COMPLETION = 0.5f;

  private static final String COLLECTION_PREFIX = EncryptionHeavyLoadTest.class.getSimpleName() + "-collection-";
  private static final String SYSTEM_OUTPUT_MARKER = "*** ";

  private volatile String collectionName;
  private volatile CloudSolrClient solrClient;
  private volatile EncryptionTestUtil testUtil;
  private volatile boolean stopTest;
  private volatile Dictionary dictionary;
  private List<Thread> threads;
  private int nextKeyIndex;
  private String keyId;
  private volatile Exception exception;
  private long startTimeNs;
  private long endTimeNs;
  private long lastDisplayTimeNs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    EncryptionTestUtil.setInstallDirProperty();
    cluster = new MiniSolrCloudCluster.Builder(2, createTempDir())
      .addConfig("config", EncryptionTestUtil.getRandomConfigPath())
      .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.shutdown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionName = COLLECTION_PREFIX + random().nextLong();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, 2, 2).process(solrClient);
    cluster.waitForActiveCollection(collectionName, 2, 4);
    testUtil = new EncryptionTestUtil(solrClient, collectionName);
    dictionary = new Dictionary.Builder().build(DICTIONARY_SIZE, random());
    threads = new ArrayList<>();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      stopTest = true;
      for (Thread thread : threads) {
        try {
          thread.join(5000);
          print(thread.getName() + " stopped");
        } catch (InterruptedException e) {
          System.err.println("Interrupted while closing " + thread.getName());
        }
      }
      startTimeNs = lastDisplayTimeNs = System.nanoTime();
      endTimeNs = startTimeNs + TimeUnit.SECONDS.toNanos(20);
      print("waiting for the final encryption completion");
      assertTrue("Timeout waiting for the final encryption completion", encrypt(keyId, true));
      print("final encryption complete");
    } finally {
      super.tearDown();
    }
  }

  @Test
  public void testReencryptionUnderHeavyConcurrentLoad() throws Exception {
    print("Starting test");
    startTimeNs = lastDisplayTimeNs = System.nanoTime();
    endTimeNs = startTimeNs + TEST_DURATION_NS;
    Random random = random();
    if (random.nextBoolean()) {
      print("preparing empty index for encryption");
      encrypt(nextKeyId(), waitForCompletion(random));
    }
    startThreads(NUM_INDEXING_THREADS, "Indexing", Indexer::new);
    startThreads(NUM_QUERYING_THREADS, "Querying", Querier::new);
    while (!isTimeElapsed()) {
      Thread.sleep(random.nextInt(RANDOM_DELAY_BETWEEN_REENCRYPTION_MS));
      encrypt(nextKeyId(), waitForCompletion(random));
    }
    long timeNs = System.nanoTime();
    if (timeNs - lastDisplayTimeNs >= TimeUnit.SECONDS.toNanos(1)) {
      print("elapsed time = " + TimeUnit.NANOSECONDS.toSeconds(timeNs - startTimeNs) + " s");
    }
    print("Stopping test");
    if (exception != null) {
      throw exception;
    }
  }

  private void startThreads(int numThreads, String namePrefix, Supplier<Runnable> runnableSupplier) {
    for (int i = 0; i < numThreads; i++) {
      String name = namePrefix + "-" + i;
      print("Start " + name);
      Thread thread = new Thread(runnableSupplier.get(), name);
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }
  }

  private boolean isTimeElapsed() {
    long timeNs = System.nanoTime();
    if (timeNs - lastDisplayTimeNs >= TimeUnit.SECONDS.toNanos(10)) {
      print("elapsed time = " + TimeUnit.NANOSECONDS.toSeconds(timeNs - startTimeNs) + " s");
      lastDisplayTimeNs = timeNs;
    }
    return timeNs >= endTimeNs;
  }

  private String nextKeyId() {
    keyId = KEY_IDS[nextKeyIndex++];
    if (nextKeyIndex == KEY_IDS.length) {
      nextKeyIndex = 0;
    }
    return keyId;
  }

  private boolean encrypt(String keyId, boolean waitForCompletion) throws Exception {
    EncryptionStatus encryptionStatus = sendEncryptionRequests(keyId);
    if (!encryptionStatus.isComplete()) {
      if (!waitForCompletion) {
        return false;
      }
      print("waiting for encryption completion for keyId=" + keyId);
      while (!encryptionStatus.isComplete()) {
        if (isTimeElapsed()) {
          return false;
        }
        Thread.sleep(500);
        encryptionStatus = sendEncryptionRequests(keyId);
      }
      print("encryption complete for keyId=" + keyId);
    }
    return true;
  }

  private boolean waitForCompletion(Random random) {
    return random.nextFloat() <= PROBABILITY_OF_WAITING_ENCRYPTION_COMPLETION;
  }

  private EncryptionStatus sendEncryptionRequests(String keyId) throws Exception {
    EncryptionStatus encryptionStatus = testUtil.encrypt(keyId);
    print("encrypt keyId=" + keyId + " => response success=" + encryptionStatus.isSuccess() + " complete=" + encryptionStatus.isComplete());
    return encryptionStatus;
  }

  private static void print(String message) {
    System.out.println(SYSTEM_OUTPUT_MARKER + message);
  }

  private static void threadPrint(String message) {
    print(Thread.currentThread().getName() + ": " + message);
  }

  private static class Dictionary {

    final List<String> terms;

    Dictionary(List<String> terms) {
      this.terms = terms;
    }

    String getTerm(Random random) {
      return terms.get(random.nextInt(terms.size()));
    }

    static class Builder {

      Dictionary build(int size, Random random) {
        Set<String> terms = new HashSet<>();
        for (int i = 0; i < size;) {
          String term = RandomStrings.randomAsciiLettersOfLengthBetween(random, 4, 12);
          if (terms.add(term)) {
            i++;
          }
        }
        return new Dictionary(new ArrayList<>(terms));
      }
    }
  }

  private class Indexer implements Runnable {

    final long seed;
    final AtomicLong docNum = new AtomicLong();

    Indexer() {
      seed = random().nextLong();
    }

    @Override
    public void run() {
      long numBatches = 0;
      long totalDocs = 0;
      long numCommits = 0;
      try {
        Random random = new Random(seed);
        while (!stopTest) {
          Thread.sleep(random.nextInt(RANDOM_DELAY_BETWEEN_INDEXING_BATCHES_MS));
          Collection<SolrInputDocument> docs = new ArrayList<>();
          for (int i = random.nextInt(RANDOM_NUM_DOCS_PER_BATCH) + 1; i > 0; i--) {
            docs.add(createDoc(random));
          }
          totalDocs += docs.size();
          solrClient.add(collectionName, docs);
          if (random.nextFloat() <= PROBABILITY_OF_COMMIT_PER_BATCH) {
            numCommits++;
            solrClient.commit(collectionName);
          }
          if (++numBatches % 10 == 0) {
            threadPrint("sent " + numBatches + " indexing batches, totalDocs=" + totalDocs + ", numCommits=" + numCommits);
          }
        }
      } catch (InterruptedException e) {
        threadPrint("Indexing interrupted");
        e.printStackTrace(System.err);
      } catch (Exception e) {
        exception = e;
        threadPrint("Indexing stopped by exception");
        e.printStackTrace(System.err);
      } finally {
        threadPrint("Stop indexing");
        threadPrint("sent " + numBatches + " indexing batches, totalDocs=" + totalDocs + ", numCommits=" + numCommits);
        stopTest = true;
      }
    }

    SolrInputDocument createDoc(Random random) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Long.toString(docNum.getAndIncrement()));
      doc.addField("text", dictionary.getTerm(random));
      return doc;
    }
  }

  private class Querier implements Runnable {

    final long seed;

    Querier() {
      seed = random().nextLong();
    }

    @Override
    public void run() {
      long totalResults = 0;
      long numQueries = 0;
      long numConsecutiveNoResults = 0;
      try {
        Random random = new Random(seed);
        while (!stopTest) {
          Thread.sleep(random.nextInt(RANDOM_DELAY_BETWEEN_QUERIES_MS));
          QueryResponse response = null;
          do {
            try {
              response = solrClient.query(collectionName, new SolrQuery(dictionary.getTerm(random)));
            } catch (Exception e) {
              // Some queries might not be parseable due to the random terms. Just retry with another term.
            }
          } while (response == null);
          int numResults = response.getResults().size();
          totalResults += numResults;
          numQueries++;
          if (numResults == 0) {
            numConsecutiveNoResults++;
          } else {
            numConsecutiveNoResults = 0;
          }
          if (numQueries % 500 == 0) {
            threadPrint("sent " + numQueries + " queries, totalResults=" + totalResults + ", numConsecutiveNoResults=" + numConsecutiveNoResults);
          }
        }
      } catch (InterruptedException e) {
        threadPrint("Querying interrupted");
        e.printStackTrace(System.err);
      } catch (Exception e) {
        exception = e;
        threadPrint("Querying stopped by exception");
        e.printStackTrace(System.err);
      } finally {
        threadPrint("Stop querying");
        threadPrint("sent " + numQueries + " queries, totalResults=" + totalResults + ", numConsecutiveNoResults=" + numConsecutiveNoResults);
        stopTest = true;
      }
    }
  }
}
