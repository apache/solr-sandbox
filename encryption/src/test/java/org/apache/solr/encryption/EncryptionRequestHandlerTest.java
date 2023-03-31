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
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.encryption.EncryptionRequestHandler.*;
import static org.apache.solr.encryption.EncryptionUtil.getKeyIdFromCommit;

/**
 * Tests {@link EncryptionRequestHandler} (re)encryption logic.
 * For a concurrent heavy load test, see {@link EncryptionHeavyLoadTest}.
 */
public class EncryptionRequestHandlerTest extends SolrCloudTestCase {

  private static final String COLLECTION_PREFIX = EncryptionRequestHandlerTest.class.getSimpleName() + "-collection-";

  private static MockEncryptionDirectory mockDir;

  private String collectionName;
  private CloudSolrClient solrClient;
  private TestUtil testUtil;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(EncryptionDirectoryFactory.PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY, MockFactory.class.getName());
    TestUtil.setInstallDirProperty();
    cluster = new MiniSolrCloudCluster.Builder(1, createTempDir())
      .addConfig("config", TestUtil.getConfigPath("collection1"))
      .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty(EncryptionDirectoryFactory.PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY);
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
    mockDir.clearMockValues();
    CollectionAdminRequest.deleteCollection(collectionName).process(solrClient);
    super.tearDown();
  }

  @Test
  public void testEncryptionFromNoKeysToOneKey_NoIndex() throws Exception {
    // Send an encrypt request with a key id on an empty index.
    NamedList<Object> response = encrypt(TestingKeyManager.KEY_ID_1);
    assertEquals(STATUS_SUCCESS, response.get(STATUS));
    assertEquals(STATE_COMPLETE, response.get(ENCRYPTION_STATE));

    // Index some documents to create a first segment.
    testUtil.indexDocsAndCommit("weather broadcast");

    // Verify that the segment is encrypted.
    mockDir.forceClearText = true;
    testUtil.assertCannotReloadCore();
    mockDir.forceClearText = false;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 1);
  }

  @Test
  public void testEncryptionFromNoKeysToOneKeyToNoKeys_NoIndex() throws Exception {
    // Send an encrypt request with a key id on an empty index.
    NamedList<Object> response = encrypt(TestingKeyManager.KEY_ID_1);
    assertEquals(STATUS_SUCCESS, response.get(STATUS));
    assertEquals(STATE_COMPLETE, response.get(ENCRYPTION_STATE));

    // Send another encrypt request with no key id, still on the empty index.
    response = encrypt(NO_KEY_ID);
    assertEquals(STATUS_SUCCESS, response.get(STATUS));
    assertEquals(STATE_COMPLETE, response.get(ENCRYPTION_STATE));

    // Index some documents to create a first segment.
    testUtil.indexDocsAndCommit("weather broadcast");

    // Verify that the segment is cleartext.
    mockDir.forceClearText = true;
    testUtil.reloadCore();
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
    mockDir.forceClearText = true;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 2);
    mockDir.forceClearText = false;

    // Send an encrypt request with a key id.
    NamedList<Object> response = encrypt(TestingKeyManager.KEY_ID_1);
    assertEquals(STATUS_SUCCESS, response.get(STATUS));
    assertEquals(STATE_PENDING, response.get(ENCRYPTION_STATE));

    waitUntilEncryptionIsComplete(TestingKeyManager.KEY_ID_1);

    // Verify that the segment is encrypted.
    mockDir.forceClearText = true;
    testUtil.assertCannotReloadCore();
    mockDir.forceClearText = false;
    mockDir.soleKeyIdAllowed = TestingKeyManager.KEY_ID_1;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 2);
    mockDir.clearMockValues();
  }

  @Test
  public void testEncryptionFromOneKeyToAnotherKey_ExistingIndex() throws Exception {
    createAndEncryptIndex();

    // Index some documents to ensure we have at least two segments.
    testUtil.indexDocsAndCommit("foggy weather");

    // Send an encrypt request with another key id.
    NamedList<Object> response = encrypt(TestingKeyManager.KEY_ID_2);
    assertEquals(STATUS_SUCCESS, response.get(STATUS));
    assertEquals(STATE_PENDING, response.get(ENCRYPTION_STATE));

    waitUntilEncryptionIsComplete(TestingKeyManager.KEY_ID_2);

    // Verify that the segment is encrypted.
    mockDir.forceClearText = true;
    testUtil.assertCannotReloadCore();
    mockDir.forceClearText = false;
    mockDir.soleKeyIdAllowed = TestingKeyManager.KEY_ID_2;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 3);
  }

  @Test
  public void testEncryptionFromOneKeyToNoKeys_ExistingIndex() throws Exception {
    createAndEncryptIndex();

    // Index some documents to ensure we have at least two segments.
    testUtil.indexDocsAndCommit("foggy weather");

    // Send an encrypt request with no key id.
    NamedList<Object> response = encrypt(NO_KEY_ID);
    assertEquals(STATUS_SUCCESS, response.get(STATUS));
    assertEquals(STATE_PENDING, response.get(ENCRYPTION_STATE));

    waitUntilEncryptionIsComplete(NO_KEY_ID);

    // Verify that the segment is cleartext.
    mockDir.forceClearText = true;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 3);
    mockDir.clearMockValues();

    // Index some documents to ensure we have at least two segments.
    testUtil.indexDocsAndCommit("cloudy weather");

    // Send an encrypt request with another key id.
    response = encrypt(TestingKeyManager.KEY_ID_2);
    assertEquals(STATUS_SUCCESS, response.get(STATUS));
    assertEquals(STATE_PENDING, response.get(ENCRYPTION_STATE));

    waitUntilEncryptionIsComplete(TestingKeyManager.KEY_ID_2);

    // Verify that the segment is encrypted.
    mockDir.forceClearText = true;
    testUtil.assertCannotReloadCore();
    mockDir.forceClearText = false;
    mockDir.soleKeyIdAllowed = TestingKeyManager.KEY_ID_2;
    testUtil.reloadCore();
    testUtil.assertQueryReturns("weather", 4);
  }

  private NamedList<Object> encrypt(String keyId) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(PARAM_KEY_ID, keyId);
    return solrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/encrypt", params));
  }

  private void waitUntilEncryptionIsComplete(String keyId) throws InterruptedException {
    RetryUtil.retryUntil("Timeout waiting for encryption completion",
                         50,
                         100,
                         TimeUnit.MILLISECONDS,
                         () -> {
                           NamedList<Object> response;
                           try {
                             response = encrypt(keyId);
                           } catch (Exception e) {
                             throw new RuntimeException(e);
                           }
                           assertEquals(STATUS_SUCCESS, response.get(STATUS));
                           return response.get(ENCRYPTION_STATE).equals(STATE_COMPLETE);
                         });
  }

  public static class MockFactory implements EncryptionDirectoryFactory.InnerFactory {
    @Override
    public EncryptionDirectory create(Directory delegate,
                                      AesCtrEncrypterFactory encrypterFactory,
                                      KeyManager keyManager) throws IOException {
      return mockDir = new MockEncryptionDirectory(delegate, encrypterFactory, keyManager);
    }
  }

  private static class MockEncryptionDirectory extends EncryptionDirectory {

    boolean forceClearText;
    String soleKeyIdAllowed;

    MockEncryptionDirectory(Directory delegate, AesCtrEncrypterFactory encrypterFactory, KeyManager keyManager)
      throws IOException {
      super(delegate, encrypterFactory, keyManager);
    }

    void clearMockValues() {
      forceClearText = false;
      soleKeyIdAllowed = null;
    }

    @Override
    public IndexInput openInput(String fileName, IOContext context) throws IOException {
      return forceClearText ? in.openInput(fileName, context) : super.openInput(fileName, context);
    }

    @Override
    protected byte[] getKeySecret(String keyRef) throws IOException {
      if (soleKeyIdAllowed != null) {
        String keyId = getKeyIdFromCommit(keyRef, getLatestCommitData().data);
        assertEquals(soleKeyIdAllowed, keyId);
      }
      return super.getKeySecret(keyRef);
    }
  }
}
