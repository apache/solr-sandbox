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
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;

import java.io.IOException;
import java.util.HashMap;
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
 * <p>
 * This test class ignores the DirectoryFactory defined in solrconfig.xml to use
 * {@link EncryptionDirectoryFactory}.
 */
public class EncryptionDirectoryTest extends SolrCloudTestCase {

  private static final String COLLECTION_PREFIX = EncryptionDirectoryTest.class.getSimpleName() + "-collection-";

  private static MockEncryptionDirectory mockDir;

  private String collectionName;
  private CloudSolrClient solrClient;
  private TestUtil testUtil;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY, MockFactory.class.getName());
    System.setProperty("solr." + PARAM_KEY_SUPPLIER_FACTORY, TestingKeySupplier.Factory.class.getName());
    TestUtil.setInstallDirProperty();
    cluster = new MiniSolrCloudCluster.Builder(1, createTempDir())
      .addConfig("config", TestUtil.getConfigPath("collection1"))
      .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty(PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY);
    System.clearProperty("solr." + PARAM_KEY_SUPPLIER_FACTORY);
    cluster.shutdown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionName = COLLECTION_PREFIX + UUID.randomUUID();
    solrClient = cluster.getSolrClient();
    solrClient.setDefaultCollection(collectionName);
    CollectionAdminRequest.createCollection(collectionName, 1, 1).process(solrClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);
    testUtil = new TestUtil(solrClient, collectionName);
  }

  @Override
  public void tearDown() throws Exception {
    if (mockDir != null) {
      mockDir.clearMockValues();
    }
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
    mockDir.clearMockValues();
    // Create 2 index segments without encryption.
    testUtil.indexDocsAndCommit("weather broadcast");
    testUtil.indexDocsAndCommit("sunny weather");
    testUtil.assertQueryReturns("weather", 2);

    // Verify that without key id, we can reload the index because it is not encrypted.
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 2);

    // Set the encryption key id in the commit user data,
    // and run an optimized commit to rewrite the index, now encrypted.
    mockDir.setKeysInCommitUserData(KEY_ID_1);
    solrClient.optimize();

    // Verify that without key id, we cannot decrypt the index anymore.
    mockDir.forceClearText = true;
    testUtil.assertCannotReloadCore();
    // Verify that with a wrong key id, we cannot decrypt the index.
    mockDir.forceClearText = false;
    mockDir.forceKeySecret = KEY_SECRET_2;
    testUtil.assertCannotReloadCore();
    // Verify that with the right key id, we can decrypt the index and search it.
    mockDir.forceKeySecret = null;
    mockDir.expectedKeySecret = KEY_SECRET_1;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 2);
    testUtil.assertQueryReturns("sunny", 1);
    mockDir.clearMockValues();
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
    mockDir.setKeysInCommitUserData(KEY_ID_1);
    testUtil.indexDocsAndCommit("foggy weather");

    // Verify that without key id, we cannot decrypt the index.
    mockDir.forceClearText = true;
    testUtil.assertCannotReloadCore();
    // Verify that with a wrong key id, we cannot decrypt the index.
    mockDir.forceClearText = false;
    mockDir.forceKeySecret = KEY_SECRET_2;
    testUtil.assertCannotReloadCore();
    // Verify that with the right key id, we can decrypt the index and search it.
    mockDir.forceKeySecret = null;
    mockDir.expectedKeySecret = KEY_SECRET_1;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 3);
    testUtil.assertQueryReturns("sunny", 1);
    mockDir.clearMockValues();
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
    mockDir.setKeysInCommitUserData(KEY_ID_1, KEY_ID_2);
    solrClient.optimize();

    // Verify that without key id, we cannot decrypt the index.
    mockDir.forceClearText = true;
    testUtil.assertCannotReloadCore();
    // Verify that with a wrong key id, we cannot decrypt the index.
    mockDir.forceClearText = false;
    mockDir.forceKeySecret = KEY_SECRET_1;
    testUtil.assertCannotReloadCore();
    // Verify that with the right key id, we can decrypt the index and search it.
    mockDir.forceKeySecret = null;
    mockDir.expectedKeySecret = KEY_SECRET_2;
    testUtil.reloadCore();
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
    mockDir.setKeysInCommitUserData(KEY_ID_1, null);
    solrClient.optimize();

    // Verify that without key id, we can reload the index because it is not encrypted.
    mockDir.forceClearText = true;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 3);
    testUtil.assertQueryReturns("sunny", 1);
  }

  public static class MockFactory implements EncryptionDirectoryFactory.InnerFactory {
    @Override
    public EncryptionDirectory create(Directory delegate,
                                      AesCtrEncrypterFactory encrypterFactory,
                                      KeySupplier keySupplier) throws IOException {
      return mockDir = new MockEncryptionDirectory(delegate, encrypterFactory, keySupplier);
    }
  }

  private static class MockEncryptionDirectory extends EncryptionDirectory {

    final KeySupplier keySupplier;
    boolean forceClearText;
    byte[] forceKeySecret;
    byte[] expectedKeySecret;

    MockEncryptionDirectory(Directory delegate, AesCtrEncrypterFactory encrypterFactory, KeySupplier keySupplier)
      throws IOException {
      super(delegate, encrypterFactory, keySupplier);
      this.keySupplier = keySupplier;
    }

    void clearMockValues() {
      commitUserData = new CommitUserData(commitUserData.segmentFileName, Map.of());
      forceClearText = false;
      forceKeySecret = null;
      expectedKeySecret = null;
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
      return forceClearText ? in.openInput(fileName, context) : super.openInput(fileName, context);
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
      if (forceKeySecret != null) {
        return forceKeySecret;
      }
      byte[] keySecret = super.getKeySecret(keyRef);
      if (expectedKeySecret != null) {
        assertArrayEquals(expectedKeySecret, keySecret);
      }
      return keySecret;
    }
  }
}
