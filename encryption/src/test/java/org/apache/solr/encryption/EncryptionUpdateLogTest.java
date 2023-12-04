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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.apache.solr.encryption.crypto.DecryptingChannelInputStream;
import org.apache.solr.encryption.crypto.EncryptingOutputStream;
import org.apache.solr.update.TransactionLog;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.solr.encryption.EncryptionRequestHandler.NO_KEY_ID;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_2;
import static org.apache.solr.encryption.EncryptionTestUtil.EncryptionStatus;
import static org.apache.solr.encryption.EncryptionUpdateLogTest.TestingEncryptionUpdateLog.*;

/**
 * Tests {@link EncryptionUpdateLog} and {@link EncryptionTransactionLog}.
 */
public class EncryptionUpdateLogTest extends SolrCloudTestCase {

  private static final String COLLECTION_PREFIX = EncryptionRequestHandlerTest.class.getSimpleName() + "-collection-";

  private static final int NUM_SHARDS = 2;
  private static final int NUM_REPLICAS = 2;

  private String collectionName;
  private CloudSolrClient solrClient;
  private EncryptionTestUtil testUtil;

  @BeforeClass
  public static void beforeClass() throws Exception {
    EncryptionTestUtil.setInstallDirProperty();
    System.setProperty("solr.updateLog", TestingEncryptionUpdateLog.class.getName());
    cluster = new MiniSolrCloudCluster.Builder(NUM_SHARDS, createTempDir())
      .addConfig("config", EncryptionTestUtil.getConfigPath("collection1"))
      .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.shutdown();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    collectionName = COLLECTION_PREFIX + UUID.randomUUID();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, NUM_SHARDS, NUM_REPLICAS).process(solrClient);
    cluster.waitForActiveCollection(collectionName, NUM_SHARDS, NUM_SHARDS * NUM_REPLICAS);
    testUtil = new EncryptionTestUtil(solrClient, collectionName);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    CollectionAdminRequest.deleteCollection(collectionName).process(solrClient);
    super.tearDown();
  }

  @Test
  public void testEncryptionFromNoKeysToOneKey() throws Exception {
    checkEncryptionFromNoKeysToOneKey(KEY_ID_1);
  }

  @Test
  public void testEncryptionFromOneKeyToAnotherKey() throws Exception {
    checkEncryptionFromOneKeyToAnotherKey(KEY_ID_1, KEY_ID_2);
  }

  @Test
  public void testEncryptionFromOneKeyToNoKeys() throws Exception {
    checkEncryptionFromOneKeyToAnotherKey(KEY_ID_1, NO_KEY_ID);
  }

  private void checkEncryptionFromNoKeysToOneKey(String keyId) throws Exception {
    EncryptionStatus encryptionStatus = testUtil.encrypt(keyId);
    assertTrue(encryptionStatus.isSuccess());
    assertTrue(encryptionStatus.isComplete());

    resetCounters();
    testUtil.indexDocsAndCommit("doc0");
    assertEquals(NUM_REPLICAS, encryptedLogWriteCount.get());
    assertEquals(0, encryptedLogReadCount.get());
    assertEquals(0, reencryptionCallCount.get());

    resetCounters();
    testUtil.reloadCores();
    assertEquals(0, encryptedLogWriteCount.get());
    assertEquals(NUM_REPLICAS, encryptedLogReadCount.get());
    assertEquals(0, reencryptionCallCount.get());
  }

  private void checkEncryptionFromOneKeyToAnotherKey(String fromKeyId, String toKeyId) throws Exception {
    checkEncryptionFromNoKeysToOneKey(fromKeyId);

    for (int i = 1 ; i <= 3; i++) {
      resetCounters();
      testUtil.indexDocsAndCommit("doc" + i);
      assertEquals(NUM_REPLICAS, encryptedLogWriteCount.get());
      assertEquals(0, encryptedLogReadCount.get());
      assertEquals(0, reencryptionCallCount.get());
    }

    resetCounters();
    EncryptionStatus encryptionStatus = testUtil.encrypt(toKeyId);
    assertTrue(encryptionStatus.isSuccess());
    testUtil.waitUntilEncryptionIsComplete(toKeyId);
    assertEquals(0, encryptedLogWriteCount.get());
    assertEquals(0, encryptedLogReadCount.get());
    assertEquals(4 * NUM_REPLICAS, reencryptionCallCount.get());

    resetCounters();
    testUtil.reloadCores();
    assertEquals(0, encryptedLogWriteCount.get());
    assertEquals(4 * NUM_REPLICAS, encryptedLogReadCount.get());
    assertEquals(0, reencryptionCallCount.get());
  }

  public static class TestingEncryptionUpdateLog extends EncryptionUpdateLog {

    static final AtomicInteger reencryptionCallCount = new AtomicInteger();
    static final AtomicInteger encryptedLogWriteCount = new AtomicInteger();
    static final AtomicInteger encryptedLogReadCount = new AtomicInteger();

    static void resetCounters() {
      reencryptionCallCount.set(0);
      encryptedLogWriteCount.set(0);
      encryptedLogReadCount.set(0);
    }

    @Override
    public TransactionLog newTransactionLog(Path tlogFile, Collection<String> globalStrings, boolean openExisting) {
      return new TestingTransactionLog(tlogFile, globalStrings, openExisting, directorySupplier);
    }

    protected void reencrypt(FileChannel inputChannel,
                             String inputKeyRef,
                             OutputStream outputStream,
                             String activeKeyRef,
                             EncryptionDirectory directory)
      throws IOException {
      reencryptionCallCount.incrementAndGet();
      super.reencrypt(inputChannel, inputKeyRef, outputStream, activeKeyRef, directory);
    }

    private static class TestingTransactionLog extends EncryptionTransactionLog {

      TestingTransactionLog(Path tlogFile,
                            Collection<String> globalStrings,
                            boolean openExisting,
                            EncryptionDirectorySupplier directorySupplier) {
        this(tlogFile, globalStrings, openExisting, directorySupplier, new IvHolder());
      }

      TestingTransactionLog(Path tlogFile,
                            Collection<String> globalStrings,
                            boolean openExisting,
                            EncryptionDirectorySupplier directorySupplier,
                            IvHolder ivHolder) {
        super(tlogFile,
              globalStrings,
              openExisting,
              new TestingEncryptionOutputStreamOpener(directorySupplier, ivHolder),
              new TestingEncryptionChannelInputStreamOpener(directorySupplier, ivHolder));
      }

      static class TestingEncryptionOutputStreamOpener extends EncryptionOutputStreamOpener {

        TestingEncryptionOutputStreamOpener(EncryptionDirectorySupplier directorySupplier, IvHolder ivHolder) {
          super(directorySupplier, ivHolder);
        }

        @Override
        protected EncryptingOutputStream createEncryptingOutputStream(OutputStream outputStream,
                                                                      long position,
                                                                      byte[] iv,
                                                                      byte[] key,
                                                                      AesCtrEncrypterFactory factory)
          throws IOException {
          encryptedLogWriteCount.incrementAndGet();
          return super.createEncryptingOutputStream(outputStream, position, iv, key, factory);
        }
      }

      static class TestingEncryptionChannelInputStreamOpener extends EncryptionChannelInputStreamOpener {

        TestingEncryptionChannelInputStreamOpener(EncryptionDirectorySupplier directorySupplier,
                                                            IvHolder ivHolder) {
          super(directorySupplier, ivHolder);
        }

        @Override
        protected DecryptingChannelInputStream createDecryptingChannelInputStream(FileChannel channel,
                                                                                  long offset,
                                                                                  long position,
                                                                                  byte[] key,
                                                                                  AesCtrEncrypterFactory factory)
          throws IOException {
          encryptedLogReadCount.incrementAndGet();
          return super.createDecryptingChannelInputStream(channel, offset, position, key, factory);
        }
      }
    }
  }
}
