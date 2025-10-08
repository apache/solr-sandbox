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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.encryption.kms.TestingKmsClient;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.TIME_ALLOWED;
import static org.apache.solr.encryption.EncryptionRequestHandler.ENCRYPTION_STATE;
import static org.apache.solr.encryption.EncryptionRequestHandler.PARAM_KEY_ID;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATUS;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATUS_SUCCESS;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_ENCRYPTION_KEY_BLOB;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
  private Boolean shouldDistributeRequests;
  private long distributionTimeoutMs;

  public EncryptionTestUtil(CloudSolrClient cloudSolrClient, String collectionName) {
    this.cloudSolrClient = cloudSolrClient;
    this.collectionName = collectionName;
  }

  public boolean shouldDistributeEncryptRequest() {
    if (shouldDistributeRequests == null) {
      setShouldDistributeRequests(random().nextBoolean());
    }
    return shouldDistributeRequests;
  }

  public EncryptionTestUtil setShouldDistributeRequests(Boolean shouldDistributeRequests) {
    this.shouldDistributeRequests = shouldDistributeRequests;
    return this;
  }

  public EncryptionTestUtil setDistributionTimeoutMs(long distributionTimeoutMs) {
    this.distributionTimeoutMs = distributionTimeoutMs;
    return this;
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
    if (shouldDistributeEncryptRequest()) {
      return encryptDistrib(params);
    }
    GenericSolrRequest encryptRequest = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/encrypt", params);
    EncryptionStatus encryptionStatus = new EncryptionStatus();
    forAllReplicas(false, replica -> {
      NamedList<?> response = requestCore(encryptRequest, replica);
      EncryptionRequestHandler.State state = EncryptionRequestHandler.State.fromValue(response.get(ENCRYPTION_STATE).toString());
      encryptionStatus.success &= response.get(STATUS).equals(STATUS_SUCCESS);
      encryptionStatus.complete &= state == EncryptionRequestHandler.State.COMPLETE;
    });
    return encryptionStatus;
  }

  private EncryptionStatus encryptDistrib(SolrParams params) throws SolrServerException, IOException {
    ModifiableSolrParams modifiableParams = new ModifiableSolrParams().set(DISTRIB, true);
    if (distributionTimeoutMs != 0 || random().nextBoolean()) {
      modifiableParams.set(TIME_ALLOWED, String.valueOf(distributionTimeoutMs));
    }
    params = SolrParams.wrapDefaults(params, modifiableParams);
    GenericSolrRequest encryptRequest = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/encrypt", params);
    NamedList<Object> response = cloudSolrClient.request(encryptRequest, collectionName);
    EncryptionRequestHandler.State state = EncryptionRequestHandler.State.fromValue(response.get(ENCRYPTION_STATE).toString());
    EncryptionStatus encryptionStatus = new EncryptionStatus();
    encryptionStatus.success = response.get(STATUS).equals(STATUS_SUCCESS);
    encryptionStatus.complete = state == EncryptionRequestHandler.State.COMPLETE;
    encryptionStatus.collectionState = state;
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
    AtomicBoolean success = new AtomicBoolean();
    RetryUtil.retryUntil("Timeout waiting for encryption completion",
                         20,
                         1000,
                         TimeUnit.MILLISECONDS,
                         () -> {
                           EncryptionStatus encryptionStatus;
                           try {
                             encryptionStatus = encrypt(keyId);
                           } catch (Exception e) {
                             throw new RuntimeException(e);
                           }
                           success.set(encryptionStatus.success);
                           return encryptionStatus.complete;
                         });
    assertTrue(success.get());
  }

  /**
   * Reloads the replicas of the collection defined in the constructor.
   * <p>
   * If {@link #shouldDistributeEncryptRequest()} returns true, then only the leader replicas are reloaded
   * (because only them receive the distributed encryption request).
   */
  public void reloadCores() {
    reloadCores(shouldDistributeEncryptRequest());
  }

  /**
   * Reloads the replicas of the collection defined in the constructor.
   *
   * @param onlyLeaders whether to only reload leader replicas, or all replicas.
   */
  public void reloadCores(boolean onlyLeaders) {
    forAllReplicas(onlyLeaders, this::reloadCore);
  }

  /**
   * Reloads a specific replica.
   */
  public void reloadCore(Replica replica) {
    try {
      CoreAdminRequest req = new CoreAdminRequest();
      req.setCoreName(replica.getCoreName());
      req.setAction(CoreAdminParams.CoreAdminAction.RELOAD);
      try (Http2SolrClient httpSolrClient = new Http2SolrClient.Builder(replica.getBaseUrl()).build()) {
        httpSolrClient.request(req);
      }
    } catch (SolrServerException | SolrException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The index cannot be reloaded. There is probably an issue with the encryption key ids.", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Verifies that the replicas of the collection defined in the constructor fail to reload.
   * <p>
   * If {@link #shouldDistributeEncryptRequest()} returns true, then only the leader replicas are reloaded
   * (because only them receive the distributed encryption request).
   */
  public void assertCannotReloadCores() {
    assertCannotReloadCores(shouldDistributeEncryptRequest());
  }

  /**
   * Verifies that the replicas of the collection defined in the constructor fail to reload.
   *
   * @param onlyLeaders whether to only reload leader replicas, or all replicas.
   */
  public void assertCannotReloadCores(boolean onlyLeaders) {
    Map<String, Integer> numReloadedPerShard = new HashMap<>();
    forAllReplicas(onlyLeaders, replica -> {
      try {
        numReloadedPerShard.putIfAbsent(replica.getShard(), 0);
        reloadCore(replica);
        numReloadedPerShard.compute(replica.getShard(), (k, v) -> v == null ? 1 : v + 1);
      } catch (SolrException e) {
        // Expected.
      }
    });
    // It is tricky to check that index is encrypted. We check that by reloading the cores, and
    // forcing the mock EncryptionDirectory to consider clear text, and attempting to open the index
    // files. But in our tests, the small index files are not in all shards. So we check here that
    // at least one shard has no cores successfully reloaded.
    for (Map.Entry<String, Integer> entry : numReloadedPerShard.entrySet()) {
      if (entry.getValue() == 0) {
        return;
      }
    }
    fail("Core reloaded whereas it was not expected to be possible");
  }

  /** Processes the given {@code action} for all replicas of the collection defined in the constructor. */
  public void forAllReplicas(boolean onlyLeaders, Consumer<Replica> action) {
    for (Slice slice : cloudSolrClient.getClusterState().getCollection(collectionName).getSlices()) {
      if (onlyLeaders) {
        action.accept(slice.getLeader());
      } else {
        for (Replica replica : slice.getReplicas()) {
          action.accept(replica);
        }
      }
    }
  }

  /** Sends the given {@link SolrRequest} to a specific replica. */
  public NamedList<Object> requestCore(SolrRequest<?> request, Replica replica) {
    try (Http2SolrClient httpSolrClient = new Http2SolrClient.Builder(replica.getCoreUrl()).build()) {
      return httpSolrClient.request(request);
    } catch (SolrServerException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Status of the encryption of potentially multiple cores. */
  public static class EncryptionStatus {

    private boolean success;
    private boolean complete;
    private EncryptionRequestHandler.State collectionState;

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

    public EncryptionRequestHandler.State getCollectionState() {
      return collectionState;
    }
  }
}
