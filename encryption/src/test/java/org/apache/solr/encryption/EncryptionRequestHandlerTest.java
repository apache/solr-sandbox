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
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.apache.solr.encryption.EncryptionDirectoryFactory.PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY;
import static org.apache.solr.encryption.EncryptionRequestHandler.NO_KEY_ID;
import static org.apache.solr.encryption.EncryptionUtil.getKeyIdFromCommit;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_2;

/**
 * Tests {@link EncryptionRequestHandler} (re)encryption logic.
 * For a concurrent heavy load test, see {@link EncryptionHeavyLoadTest}.
 */
public class EncryptionRequestHandlerTest extends SolrCloudTestCase {

  private static final String COLLECTION_PREFIX = EncryptionRequestHandlerTest.class.getSimpleName() + "-collection-";

  private static volatile boolean forceClearText;
  private static volatile String soleKeyIdAllowed;

  private String collectionName;
  private CloudSolrClient solrClient;
  private EncryptionTestUtil testUtil;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY, MockFactory.class.getName());
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
    collectionName = COLLECTION_PREFIX + UUID.randomUUID();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, 2, 2).process(solrClient);
    cluster.waitForActiveCollection(collectionName, 2, 4);
    testUtil = createEncryptionTestUtil(solrClient, collectionName);
  }

  protected EncryptionTestUtil createEncryptionTestUtil(CloudSolrClient solrClient, String collectionName) {
    return new EncryptionTestUtil(solrClient, collectionName);
  }

  @Override
  public void tearDown() throws Exception {
    clearMockValues();
    CollectionAdminRequest.deleteCollection(collectionName).process(solrClient);
    super.tearDown();
  }

  @Test
  public void testEncryptionFromNoKeysToOneKey_NoIndex() throws Exception {
    // Send an encrypt request with a key id on an empty index.
    testUtil.encryptAndExpectCompletion(KEY_ID_1);

    // Index some documents to create a first segment.
    testUtil.indexDocsAndCommit("weather broadcast");

    // Verify that the segment is encrypted.
    forceClearText = true;
    testUtil.assertCannotReloadCores();
    forceClearText = false;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 1);
  }

  @Test
  public void testEncryptionFromNoKeysToOneKeyToNoKeys_NoIndex() throws Exception {
    // Send an encrypt request with a key id on an empty index.
    testUtil.encryptAndExpectCompletion(KEY_ID_1);

    // Send another encrypt request with no key id, still on the empty index.
    testUtil.encryptAndExpectCompletion(NO_KEY_ID);

    // Index some documents to create a first segment.
    testUtil.indexDocsAndCommit("weather broadcast");

    // Verify that the segment is cleartext.
    forceClearText = true;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 1);
  }

  @Test
  public void testEncryptionFromNoKeysToOneKey_ExistingIndex() throws Exception {
    createAndEncryptIndex();
  }

  private void createAndEncryptIndex() throws Exception {
    // Index some documents to create multiple segments.
    testUtil.indexDocsAndCommit("weather broadcast");
    testUtil.indexDocsAndCommit("sunny weather");
    // Verify that the segments are cleartext.
    forceClearText = true;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 2);
    forceClearText = false;

    // Send an encrypt request with a key id.
    testUtil.encryptAndWaitForCompletion(KEY_ID_1);

    // Verify that the segment is encrypted.
    forceClearText = true;
    testUtil.assertCannotReloadCores();
    forceClearText = false;
    soleKeyIdAllowed = KEY_ID_1;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 2);
    clearMockValues();
  }

  @Test
  public void testEncryptionFromOneKeyToAnotherKey_ExistingIndex() throws Exception {
    createAndEncryptIndex();

    // Index some documents to ensure we have at least two segments.
    testUtil.indexDocsAndCommit("foggy weather");

    // Send an encrypt request with another key id.
    testUtil.encryptAndWaitForCompletion(KEY_ID_2);

    // Verify that the segment is encrypted.
    forceClearText = true;
    testUtil.assertCannotReloadCores();
    forceClearText = false;
    soleKeyIdAllowed = KEY_ID_2;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 3);
  }

  @Test
  public void testEncryptionFromOneKeyToNoKeys_ExistingIndex() throws Exception {
    createAndEncryptIndex();

    // Index some documents to ensure we have at least two segments.
    testUtil.indexDocsAndCommit("foggy weather");

    // Send an encrypt request with no key id.
    testUtil.encryptAndWaitForCompletion(NO_KEY_ID);

    // Verify that the segment is cleartext.
    forceClearText = true;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 3);
    clearMockValues();

    // Index some documents to ensure we have at least two segments.
    testUtil.indexDocsAndCommit("cloudy weather");

    // Send an encrypt request with another key id.
    testUtil.encryptAndWaitForCompletion(KEY_ID_2);

    // Verify that the segment is encrypted.
    forceClearText = true;
    testUtil.assertCannotReloadCores();
    forceClearText = false;
    soleKeyIdAllowed = KEY_ID_2;
    testUtil.reloadCores();
    testUtil.assertQueryReturns("weather", 4);
  }

  @Test
  public void testEncryptionAndNodeRestart() throws Exception {
    // Send an encrypt request with a key id on an empty index.
    testUtil.encryptAndExpectCompletion(KEY_ID_1);

    // Index some documents but do not commit.
    testUtil.indexDocsNoCommit("weather broadcast");

    // Restart the Solr nodes.
    restartSolrServer(0);
    restartSolrServer(1);

    testUtil.commit();

    // Verify that the segment is encrypted.
    testUtil.assertQueryReturns("weather", 1);
  }

  private void restartSolrServer(int nodeIndex) throws Exception {
    // Restart the Solr node
    JettySolrRunner node = cluster.stopJettySolrRunner(nodeIndex);
    cluster.waitForJettyToStop(node);
    cluster.startJettySolrRunner(node);
    cluster.waitForNode(node, 30);
  }

  private static void clearMockValues() {
    forceClearText = false;
    soleKeyIdAllowed = null;
  }

  public static class MockFactory implements EncryptionDirectoryFactory.InnerFactory {
    @Override
    public EncryptionDirectory create(Directory delegate,
                                      AesCtrEncrypterFactory encrypterFactory,
                                      KeySupplier keySupplier) throws IOException {
      return new MockEncryptionDirectory(delegate, encrypterFactory, keySupplier);
    }
  }

  private static class MockEncryptionDirectory extends EncryptionDirectory {

    MockEncryptionDirectory(Directory delegate, AesCtrEncrypterFactory encrypterFactory, KeySupplier keySupplier)
      throws IOException {
      super(delegate, encrypterFactory, keySupplier);
    }

    @Override
    public IndexInput openInput(String fileName, IOContext context) throws IOException {
      return forceClearText ? in.openInput(fileName, context) : super.openInput(fileName, context);
    }

    @Override
    protected byte[] getKeySecret(String keyRef) throws IOException {
      if (soleKeyIdAllowed != null) {
        String keyId = getKeyIdFromCommit(keyRef, ((TestCommitUserData) getLatestCommitData()).getData());
        assertEquals(soleKeyIdAllowed, keyId);
      }
      return super.getKeySecret(keyRef);
    }

    @Override
    protected CommitUserData createCommitUserData(String segmentFileName, Map<String, String> data) {
      return new TestCommitUserData(segmentFileName, data);
    }

    private static class TestCommitUserData extends CommitUserData {

      TestCommitUserData(String segmentFileName, Map<String, String> data) {
        super(segmentFileName, data);
      }

      Map<String, String> getData() {
        return data;
      }
    }
  }
}
