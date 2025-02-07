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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.solr.encryption.EncryptionDirectoryFactory.PARAM_KEY_SUPPLIER_FACTORY;
import static org.apache.solr.encryption.EncryptionDirectoryFactory.PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY;
import static org.apache.solr.encryption.TestingEncryptionRequestHandler.MOCK_COOKIE_PARAMS;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_2;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_SECRET_1;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_SECRET_2;

/**
 * Tests {@link EncryptionDirectory}.
 */
public class EncryptionDirectoryTest extends SolrCloudTestCase {

  private static final String COLLECTION_PREFIX = EncryptionDirectoryTest.class.getSimpleName() + "-collection-";

  private String collectionName;
  private CloudSolrClient solrClient;
  private EncryptionTestUtil testUtil;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY, MockFactory.class.getName());
    System.setProperty("solr." + PARAM_KEY_SUPPLIER_FACTORY, TestingKeySupplier.Factory.class.getName());
    EncryptionTestUtil.setInstallDirProperty();
    cluster = new MiniSolrCloudCluster.Builder(2, createTempDir())
      .addConfig("config", EncryptionTestUtil.getConfigPath("collection1"))
      .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.shutdown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionName = COLLECTION_PREFIX + UUID.randomUUID();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, 2, 2).process(solrClient);
    cluster.waitForActiveCollection(collectionName, 2, 4);
    testUtil = new EncryptionTestUtil(solrClient, collectionName)
        .setShouldDistributeRequests(false);
  }

  @Override
  public void tearDown() throws Exception {
    MockFactory.clearMockValues();
    CollectionAdminRequest.deleteCollection(collectionName).process(solrClient);
    super.tearDown();
  }

  /**
   * Verifies the encryption of an index, moving from no keys to one key.
   */
  @Test
  public void testEncryptionFromNoKeysToOneKey() throws Exception {
    indexAndEncryptOneSegment();
  }

  /**
   * Starts from an empty index, indexes two documents in two segments, then encrypt the index
   * with {@link TestingKeySupplier#KEY_ID_1}. The resulting encrypted index is composed of one segment.
   */
  private void indexAndEncryptOneSegment() throws Exception {
    // Start with no key ids defined in the latest commit metadata.
    MockFactory.clearMockValues();
    // Create 2 index segments without encryption.
    testUtil.indexDocsAndCommit("weather broadcast");
    testUtil.indexDocsAndCommit("sunny weather");
    testUtil.indexDocsAndCommit("foo");
    testUtil.indexDocsAndCommit("bar");
    testUtil.assertQueryReturns("weather", 2);

    // Verify that without key id, we can reload the index because it is not encrypted.
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 2);

    // Set the encryption key id in the commit user data,
    // and run an optimized commit to rewrite the index, now encrypted.
    MockFactory.setKeysInCommitUserData(KEY_ID_1);
    optimizeCommit();

    // Verify that without key id, we cannot decrypt the index anymore.
    MockFactory.forceClearText = true;
    testUtil.assertCannotReloadCores();
    // Verify that with a wrong key id, we cannot decrypt the index.
    MockFactory.forceClearText = false;
    MockFactory.forceKeySecret = KEY_SECRET_2;
    testUtil.assertCannotReloadCores();
    // Verify that with the right key id, we can decrypt the index and search it.
    MockFactory.forceKeySecret = null;
    MockFactory.expectedKeySecrets = List.of(KEY_SECRET_1);
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 2);
    testUtil.assertQueryReturns("sunny", 1);
    MockFactory.clearMockValues();
  }

  /**
   * Verifies an encrypted index cannot be loaded without the right encryption key,
   * and that we can search the index if we have the right encryption key.
   */
  @Test
  public void testIndexingAndQueryingWithEncryption() throws Exception {
    indexAndEncryptTwoSegments();
  }

  /**
   * Creates an index encrypted with {@link TestingKeySupplier#KEY_ID_1} and containing two segments.
   */
  private void indexAndEncryptTwoSegments() throws Exception {
    // Prepare an encrypted index with one segment.
    indexAndEncryptOneSegment();

    // Create 1 new segment with the same encryption key id.
    MockFactory.setKeysInCommitUserData(KEY_ID_1);
    testUtil.indexDocsAndCommit("foggy weather");
    testUtil.indexDocsAndCommit("boo");

    // Verify that without key id, we cannot decrypt the index.
    MockFactory.forceClearText = true;
    testUtil.assertCannotReloadCores();
    // Verify that with a wrong key id, we cannot decrypt the index.
    MockFactory.forceClearText = false;
    MockFactory.forceKeySecret = KEY_SECRET_2;
    testUtil.assertCannotReloadCores();
    // Verify that with the right key id, we can decrypt the index and search it.
    MockFactory.forceKeySecret = null;
    MockFactory.expectedKeySecrets = List.of(KEY_SECRET_1);
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 3);
    testUtil.assertQueryReturns("sunny", 1);
    MockFactory.clearMockValues();
  }

  /**
   * Verifies the re-encryption of an index, moving from one key to another key.
   */
  @Test
  public void testReEncryptionFromOneKeyToAnotherKey() throws Exception {
    // Prepare an encrypted index with two segments.
    indexAndEncryptTwoSegments();

    // Set the new encryption key id in the commit user data,
    // and run an optimized commit to rewrite the index, now encrypted with the new key.
    MockFactory.setKeysInCommitUserData(KEY_ID_1, KEY_ID_2);
    optimizeCommit();

    // Verify that without key id, we cannot decrypt the index.
    MockFactory.forceClearText = true;
    testUtil.assertCannotReloadCores();
    // Verify that with a wrong key id, we cannot decrypt the index.
    MockFactory.forceClearText = false;
    MockFactory.forceKeySecret = KEY_SECRET_1;
    testUtil.assertCannotReloadCores();
    // Verify that with the right key id, we can decrypt the index and search it.
    MockFactory.forceKeySecret = null;
    MockFactory.expectedKeySecrets = List.of(KEY_SECRET_1, KEY_SECRET_2);
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 3);
    testUtil.assertQueryReturns("sunny", 1);
  }

  /**
   * Verifies the decryption of an index, moving from one key to no keys.
   */
  @Test
  public void testDecryptionFromOneKeyToNoKeys() throws Exception {
    // Prepare an encrypted index with two segments.
    indexAndEncryptTwoSegments();

    // Remove the active key parameter from the commit user data,
    // and run an optimized commit to rewrite the index, now cleartext with no keys.
    MockFactory.setKeysInCommitUserData(KEY_ID_1, null);
    optimizeCommit();

    // Verify that without key id, we can reload the index because it is not encrypted.
    MockFactory.forceClearText = true;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 3);
    testUtil.assertQueryReturns("sunny", 1);
  }

  /**
   * Sends an {@link UpdateRequest} with optimize to all replicas. If there are two segments or more,
   * then all segments are merged into one, ensuring here that we encrypt all the index data.
   * <p>
   * This is not what should be done to encrypt. The real request should be sent to the
   * {@link EncryptionRequestHandler}, but this test is designed to work independently.
   */
  private void optimizeCommit() {
    testUtil.forAllReplicas(false, replica -> {
      UpdateRequest request = new UpdateRequest();
      request.setAction(UpdateRequest.ACTION.OPTIMIZE, true, true, 1);
      testUtil.requestCore(request, replica);
    });
  }

  public static class MockFactory implements EncryptionDirectoryFactory.InnerFactory {

    static final List<MockEncryptionDirectory> mockDirs = new ArrayList<>();

    static boolean forceClearText;
    static byte[] forceKeySecret;
    static List<byte[]> expectedKeySecrets;

    static void clearMockValues() {
      forceClearText = false;
      forceKeySecret = null;
      expectedKeySecrets = null;
      for (MockEncryptionDirectory mockDir : mockDirs) {
        mockDir.clearMockValues();
      }
    }

    static void setKeysInCommitUserData(String... keyIds) throws IOException {
      for (MockEncryptionDirectory mockDir : mockDirs) {
        mockDir.setKeysInCommitUserData(keyIds);
      }
    }

    @Override
    public EncryptionDirectory create(Directory delegate,
                                      AesCtrEncrypterFactory encrypterFactory,
                                      KeySupplier keySupplier) throws IOException {
      MockEncryptionDirectory mockDir = new MockEncryptionDirectory(delegate, encrypterFactory, keySupplier);
      mockDirs.add(mockDir);
      return mockDir;
    }
  }

  private static class MockEncryptionDirectory extends EncryptionDirectory {

    final KeySupplier keySupplier;

    MockEncryptionDirectory(Directory delegate, AesCtrEncrypterFactory encrypterFactory, KeySupplier keySupplier)
      throws IOException {
      super(delegate, encrypterFactory, keySupplier);
      this.keySupplier = keySupplier;
    }

    void clearMockValues() {
      commitUserData = new CommitUserData(commitUserData.segmentFileName, Map.of());
    }

    /**
     * Clears the commit user data, then sets the provided key ids. The last key id is the active one.
     */
    void setKeysInCommitUserData(String... keyIds) throws IOException {
      Map<String, String> data = new HashMap<>();
      for (String keyId : keyIds) {
        if (keyId == null) {
          EncryptionUtil.removeActiveKeyRefFromCommit(data);
        } else {
          EncryptionUtil.setNewActiveKeyIdInCommit(keyId, keySupplier.getKeyCookie(keyId, MOCK_COOKIE_PARAMS), data);
        }
      }
      commitUserData = new CommitUserData(commitUserData.segmentFileName, data);
    }

    @Override
    public IndexInput openInput(String fileName, IOContext context) throws IOException {
      return MockFactory.forceClearText ? in.openInput(fileName, context) : super.openInput(fileName, context);
    }

    @Override
    protected CommitUserData readLatestCommitUserData() {
      // Keep the same data map because it contains the mock values for the test,
      // so the test is easier to write and clearer to read.
      Map<String, String> data = commitUserData == null ? new HashMap<>() : commitUserData.data;
      return new CommitUserData("test", data);
    }

    @Override
    protected byte[] getKeySecret(String keyRef) throws IOException {
      if (MockFactory.forceKeySecret != null) {
        return MockFactory.forceKeySecret;
      }
      byte[] keySecret = super.getKeySecret(keyRef);
      if (MockFactory.expectedKeySecrets != null) {
        boolean keySecretMatches = false;
        for (byte[] expectedKeySecret : MockFactory.expectedKeySecrets) {
          if (Arrays.equals(expectedKeySecret, keySecret)) {
            keySecretMatches = true;
            break;
          }
        }
        assertTrue(keySecretMatches);
      }
      return keySecret;
    }
  }
}
