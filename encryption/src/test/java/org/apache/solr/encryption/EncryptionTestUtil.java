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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.encryption.kms.TestingKmsClient;
import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.apache.solr.encryption.EncryptionRequestHandler.ENCRYPTION_STATE;
import static org.apache.solr.encryption.EncryptionRequestHandler.PARAM_KEY_ID;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATE_COMPLETE;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATUS;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATUS_SUCCESS;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_ENCRYPTION_KEY_BLOB;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility methods for encryption tests.
 */
public class EncryptionTestUtil {

  public static final String TENANT_ID = "tenantIdSolr";
  public static final String KEY_BLOB = "{" +
          "\"keyId\":\"%s\"," +
          "\"keyVersion\":\"0-a-4-a-2\"," +
          "\"cipherBlob\":\"a+K/8+p+l0\"," +
          "\"iv\":\"A/k\"," +
          "\"algorithm\":\"AES-GCM\"," +
          "\"auth\":\"Q-Z\"," +
          "}";

  private final CloudSolrClient cloudSolrClient;
  private final String collectionName;
  private int docId;

  public EncryptionTestUtil(CloudSolrClient cloudSolrClient, String collectionName) {
    this.cloudSolrClient = cloudSolrClient;
    this.collectionName = collectionName;
  }

  /**
   * Sets the "solr.install.dir" system property.
   */
  public static void setInstallDirProperty() {
    System.setProperty("solr.install.dir", SolrTestCaseJ4.getFile("..").getPath());
  }

  /**
   * Gets the path to the encryption module test config.
   */
  public static Path getConfigPath() {
    return getConfigPath("");
  }

  /**
   * Gets the path of a specific sub-dir of the encryption module test config.
   */
  public static Path getConfigPath(String configDir) {
    return SolrTestCaseJ4.getFile("src/test/resources/configs/" + configDir).toPath();
  }

  /**
   * Gets the path of random sub-dir of the encryption module test config.
   */
  public static Path getRandomConfigPath() {
    return getConfigPath(random().nextBoolean() ? "collection1" : "kms");
  }

  /**
   * Adds one doc per provided text, and commits.
   */
  public void indexDocsAndCommit(String... texts) throws Exception {
    indexDocsNoCommit(texts);
    commit();
  }

  /**
   * Adds one doc per provided text, but does not commit.
   */
  public void indexDocsNoCommit(String... texts) throws Exception {
    for (String text : texts) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Integer.toString(docId++));
      doc.addField("text", text);
      cloudSolrClient.add(collectionName, doc);
    }
  }

  /**
   * Commits.
   */
  public void commit() throws Exception {
    cloudSolrClient.commit(collectionName);
  }

  /**
   * Verifies that the provided query returns the expected number of results.
   */
  public void assertQueryReturns(String query, int expectedNumResults) throws Exception {
    QueryResponse response = cloudSolrClient.query(collectionName, new SolrQuery(query));
    assertEquals(expectedNumResults, response.getResults().size());
  }

  public EncryptionStatus encrypt(String keyId) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(PARAM_KEY_ID, keyId);
    params.set(PARAM_TENANT_ID, TENANT_ID);
    params.set(PARAM_ENCRYPTION_KEY_BLOB, generateKeyBlob(keyId));
    GenericSolrRequest encryptRequest = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/encrypt", params);
    EncryptionStatus encryptionStatus = new EncryptionStatus();
    forAllReplicas(replica -> {
      NamedList<Object> response = requestCore(encryptRequest, replica);
      encryptionStatus.success &= response.get(STATUS).equals(STATUS_SUCCESS);
      encryptionStatus.complete &= response.get(ENCRYPTION_STATE).equals(STATE_COMPLETE);
    });
    return encryptionStatus;
  }

  private String generateKeyBlob(String keyId) throws Exception {
    return TestingKmsClient.singleton == null ?
            generateMockKeyBlob(keyId)
            : TestingKmsClient.singleton.generateKeyBlob(keyId, TENANT_ID);
  }

  public static String generateMockKeyBlob(String keyId) {
    return String.format(KEY_BLOB, keyId);
  }

  public void encryptAndExpectCompletion(String keyId) throws Exception {
    encryptAndCheck(keyId, true);
  }

  public void encryptAndWaitForCompletion(String keyId) throws Exception {
    encryptAndCheck(keyId, false);
    waitUntilEncryptionIsComplete(keyId);
  }

  private void encryptAndCheck(String keyId, boolean expectComplete) throws Exception {
    EncryptionStatus encryptionStatus = encrypt(keyId);
    assertTrue(encryptionStatus.isSuccess());
    assertEquals(expectComplete, encryptionStatus.isComplete());
  }

  public void waitUntilEncryptionIsComplete(String keyId) throws InterruptedException {
    RetryUtil.retryUntil("Timeout waiting for encryption completion",
                         50,
                         100,
                         TimeUnit.MILLISECONDS,
                         () -> {
                           EncryptionStatus encryptionStatus;
                           try {
                             encryptionStatus = encrypt(keyId);
                           } catch (Exception e) {
                             throw new RuntimeException(e);
                           }
                           assertTrue(encryptionStatus.success);
                           return encryptionStatus.complete;
                         });
  }

  /**
   * Reloads the leader replica core of the first shard of the collection.
   */
  public void reloadCores() throws Exception {
    try {
      forAllReplicas(replica -> {
        try {
          CoreAdminRequest req = new CoreAdminRequest();
          req.setBasePath(replica.getBaseUrl());
          req.setCoreName(replica.getCoreName());
          req.setAction(CoreAdminParams.CoreAdminAction.RELOAD);
          try (Http2SolrClient httpSolrClient = new Http2SolrClient.Builder(replica.getBaseUrl()).build()) {
            httpSolrClient.request(req);
          }
        } catch (SolrServerException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (SolrException e) {
      throw new CoreReloadException("The index cannot be reloaded. There is probably an issue with the encryption key ids.", e);
    }
  }

  /**
   * Verifies that {@link #reloadCores()} fails.
   */
  public void assertCannotReloadCores() throws Exception {
    try {
      reloadCores();
      Assert.fail("Core reloaded whereas it was not expected to be possible");
    } catch (CoreReloadException e) {
      // Expected.
    }
  }

  /** Processes the given {@code action} for all replicas of the collection defined in the constructor. */
  public void forAllReplicas(Consumer<Replica> action) {
    for (Slice slice : cloudSolrClient.getClusterState().getCollection(collectionName).getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        action.accept(replica);
      }
    }
  }

  /** Sends the given {@link SolrRequest} to a specific replica. */
  public NamedList<Object> requestCore(SolrRequest<?> request, Replica replica) {
    request.setBasePath(replica.getCoreUrl());
    try (Http2SolrClient httpSolrClient = new Http2SolrClient.Builder(replica.getBaseUrl()).build()) {
      return httpSolrClient.request(request);
    } catch (SolrServerException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class CoreReloadException extends Exception {
    CoreReloadException(String msg, SolrException cause) {
      super(msg, cause);
    }
  }

  /** Status of the encryption of potentially multiple cores. */
  public static class EncryptionStatus {

    private boolean success;
    private boolean complete;

    private EncryptionStatus() {
      this.success = true;
      this.complete = true;
    }

    public boolean isSuccess() {
      return success;
    }

    public boolean isComplete() {
      return complete;
    }
  }
}
