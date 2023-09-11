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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.solr.encryption.EncryptionRequestHandler.ENCRYPTION_STATE;
import static org.apache.solr.encryption.EncryptionRequestHandler.PARAM_KEY_ID;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATE_COMPLETE;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATUS;
import static org.apache.solr.encryption.EncryptionRequestHandler.STATUS_SUCCESS;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;

/**
 * Tests {@link EncryptionUpdateLog} and {@link EncryptionTransactionLog}.
 */
public class EncryptionUpdateLogTest extends SolrCloudTestCase {

  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 4;
  private static final long TIMEOUT = DEFAULT_TIMEOUT;

  private String collectionName;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_SHARDS * NUM_REPLICAS)
      .addConfig("config", TestUtil.getConfigPath("collection1"))
      .configure();
  }

  @Before
  public void createCollection() throws Exception {
    collectionName = "collection" + UUID.randomUUID();
    CollectionAdminRequest.createCollection(collectionName, "config", NUM_SHARDS, NUM_REPLICAS)
      .processAndWait(cluster.getSolrClient(), TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
      collectionName, cluster.getZkStateReader(), false, true, TIMEOUT);
  }

  @After
  public void deleteCollection() throws Exception {
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
  }

  @Test
  public void testUpdateLogVersionsWithEncryption() throws Exception {

    List<SolrClient> solrClients = new ArrayList<>();
    int nonLeaderIndex = 0;
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      if (!jettySolrRunner
        .getBaseUrl()
        .toString()
        .equals(getCollectionState(collectionName).getLeader("shard1").getBaseUrl())) {
        nonLeaderIndex = solrClients.size();
      }
      solrClients.add(jettySolrRunner.newClient());
    }

    EncryptionStatus encryptionStatus = encrypt(KEY_ID_1, solrClients);
    assertTrue(encryptionStatus.statusSuccess);
    assertTrue(encryptionStatus.stateComplete);

    new UpdateRequest()
      .add(sdoc("id", "0", "text", "zero"))
      .commit(cluster.getSolrClient(), collectionName);

    for (SolrClient solrClient : solrClients) {
      checkNumUpdates(solrClient, 1);
    }

    cluster.getJettySolrRunner(nonLeaderIndex).stop();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
      collectionName, cluster.getZkStateReader(), false, true, TIMEOUT);

    new UpdateRequest()
      .add(sdoc("id", "1", "text", "one"))
      .deleteById("2")
      .deleteByQuery("text:three")
      .commit(cluster.getSolrClient(), collectionName);

    cluster.getJettySolrRunner(nonLeaderIndex).start();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
      collectionName, cluster.getZkStateReader(), false, true, TIMEOUT);

    for (SolrClient solrClient : solrClients) {
      checkNumUpdates(solrClient, 4);
    }

    for (SolrClient solrClient : solrClients) {
      solrClient.close();
    }
  }

  private EncryptionStatus encrypt(String keyId, List<SolrClient> solrClients) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(PARAM_KEY_ID, keyId);

    boolean statusSuccess = true;
    boolean stateComplete = true;
    for (SolrClient solrClient : solrClients) {
      NamedList<Object> response = solrClient.request(
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/encrypt", params), collectionName);
      statusSuccess &= STATUS_SUCCESS.equals(response.get(STATUS));
      stateComplete &= STATE_COMPLETE.equals(response.get(ENCRYPTION_STATE));
    }
    return new EncryptionStatus(statusSuccess, stateComplete);
  }

  @SuppressWarnings("unchecked")
  private void checkNumUpdates(SolrClient solrClient, int numExpected) throws Exception {

    final QueryRequest reqV = new QueryRequest(params("qt", "/get", "getVersions", "12345"));
    final NamedList<?> rspV = solrClient.request(reqV, collectionName);
    final List<Long> versions = (List<Long>) rspV.get("versions");
    assertEquals(versions.toString(), numExpected, versions.size());
    if (numExpected == 0) {
      return;
    }

    final Deque<Long> absVersions =
      versions.stream().map(Math::abs).sorted().collect(Collectors.toCollection(ArrayDeque::new));
    final Long minVersion = absVersions.getFirst();
    final Long maxVersion = absVersions.getLast();

    for (boolean skipDbq : new boolean[] {false, true}) {
      final QueryRequest reqU =
        new QueryRequest(
          params(
            "qt",
            "/get",
            "getUpdates",
            minVersion + "..." + maxVersion,
            "skipDbq",
            Boolean.toString(skipDbq)));
      final NamedList<?> rspU = solrClient.request(reqU, collectionName);
      final List<?> updatesList = (List<?>) rspU.get("updates");
      assertEquals(updatesList.toString(), numExpected, updatesList.size());
    }
  }

  private static class EncryptionStatus {

    final boolean statusSuccess;
    final boolean stateComplete;

    EncryptionStatus(boolean statusSuccess, boolean stateComplete) {
      this.statusSuccess = statusSuccess;
      this.stateComplete = stateComplete;
    }
  }
}
