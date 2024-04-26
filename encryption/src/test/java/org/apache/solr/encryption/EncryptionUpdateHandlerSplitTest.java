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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.update.SplitIndexCommand;
import org.apache.solr.update.UpdateHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.apache.solr.encryption.CommitUtil.readLatestCommit;
import static org.apache.solr.encryption.EncryptionUtil.getKeyIdFromCommit;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;

/**
 * Tests {@link EncryptionUpdateHandler} commit data transfer during shard split.
 */
public class EncryptionUpdateHandlerSplitTest extends SolrCloudTestCase {

  private static final String COLLECTION_PREFIX = EncryptionUpdateHandlerSplitTest.class.getSimpleName() + "-collection-";

  private String collectionName;
  private EncryptionTestUtil testUtil;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.updateHandler", TestEncryptionUpdateHandler.class.getName());
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
    CollectionAdminRequest
      .createCollection(collectionName, 2, 1)
      .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 2, 2);
    testUtil = new EncryptionTestUtil(cluster.getSolrClient(), collectionName);
  }

  @Override
  public void tearDown() throws Exception {
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    super.tearDown();
  }

  @Test
  public void testSplitTransfersCommitData() throws Exception {
    // Index some documents to create a first encrypted segment.
    testUtil.encryptAndExpectCompletion(KEY_ID_1);
    testUtil.indexDocsAndCommit("weather broadcast");

    SolrIndexSplitter.SplitMethod splitMethod = random().nextBoolean() ?
      SolrIndexSplitter.SplitMethod.LINK : SolrIndexSplitter.SplitMethod.REWRITE;
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
      .setNumSubShards(2)
      .setShardName("shard1")
      .setSplitMethod(splitMethod.toLower());
    splitShard.process(cluster.getSolrClient());
    waitForState("Timed out waiting for sub-shards to be active",
        collectionName,
        SolrCloudTestCase.activeClusterShape(3, 4));

    assertNotNull(TestEncryptionUpdateHandler.cmd);

    if (TestEncryptionUpdateHandler.cmd.cores != null) {
      // Check the sub-shard cores if we have them (LINK split method).
      for (SolrCore subCore : TestEncryptionUpdateHandler.cmd.cores) {
        assertEquals(KEY_ID_1, getKeyIdFromCommitData(subCore));
      }
    }
  }

  private static String getKeyIdFromCommitData(SolrCore core) throws IOException {
    Map<String, String> commitData = readLatestCommit(core).getUserData();
    assertNotNull(commitData);
    return getKeyIdFromCommit("0", commitData);
  }

  /**
   * Captures the {@link SplitIndexCommand} for validation.
   */
  public static class TestEncryptionUpdateHandler extends EncryptionUpdateHandler {

    static volatile SplitIndexCommand cmd;

    public TestEncryptionUpdateHandler(SolrCore core) {
      super(core);
    }

    public TestEncryptionUpdateHandler(SolrCore core, UpdateHandler updateHandler) {
      super(core, updateHandler);
    }

    @Override
    public void split(SplitIndexCommand cmd) throws IOException {
      TestEncryptionUpdateHandler.cmd = cmd;
      // Check the parent core commit data here while it is still active.
      SolrCore parentCore = cmd.getReq().getSearcher().getCore();
      assertEquals(KEY_ID_1, getKeyIdFromCommitData(parentCore));
      super.split(cmd);
    }
  }
}
