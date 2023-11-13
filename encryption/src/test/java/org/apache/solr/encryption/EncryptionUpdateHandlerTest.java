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

import org.apache.lucene.index.SegmentInfos;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.CommitUpdateCommand;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.solr.encryption.CommitUtil.readLatestCommit;

/**
 * Tests {@link EncryptionUpdateHandler}.
 */
public class EncryptionUpdateHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    EncryptionTestUtil.setInstallDirProperty();
    initCore("solrconfig.xml", "schema.xml", EncryptionTestUtil.getConfigPath().toString());
  }

  /**
   * Verifies that when a commit command contains some transferable user data,
   * it should allow an empty commit with no change, to persist the
   * commit-transferable user data.
   */
  @Test
  public void testEmptyCommitWithTransferableUserData() throws Exception {
    // Given an empty core.
    SolrCore core = h.getCore();
    SegmentInfos segmentInfos = readLatestCommit(core);
    assertEquals(0, segmentInfos.size());
    assertTrue(segmentInfos.getUserData().isEmpty());

    // When we commit with no change but with commit-transferable data.
    SolrQueryRequest req = new LocalSolrQueryRequest(core, Collections.emptyMap());
    CommitUpdateCommand commitCmd = new CommitUpdateCommand(req, false);
    commitCmd.commitData = new HashMap<>();
    String transferableDataKey1 = EncryptionUpdateHandler.TRANSFERABLE_COMMIT_DATA + "key1";
    String transferableDataValue1 = "myValue1";
    commitCmd.commitData.put(transferableDataKey1, transferableDataValue1);
    req.getCore().getUpdateHandler().commit(commitCmd);

    // Then the empty commit is persisted with the data.
    segmentInfos = readLatestCommit(core);
    assertEquals(0, segmentInfos.size());
    assertEquals(transferableDataValue1, segmentInfos.getUserData().get(transferableDataKey1));

    // When we commit again with different commit-transferable data.
    commitCmd = new CommitUpdateCommand(req, false);
    commitCmd.commitData = new HashMap<>();
    String transferableDataKey2 = EncryptionUpdateHandler.TRANSFERABLE_COMMIT_DATA + "key2";
    String transferableDataValue2 = "myValue2";
    commitCmd.commitData.put(transferableDataKey2, transferableDataValue2);
    req.getCore().getUpdateHandler().commit(commitCmd);

    // Then the empty commit is persisted and the commit data is replaced,
    // it does not add up.
    segmentInfos = readLatestCommit(core);
    assertEquals(0, segmentInfos.size());
    assertNull(segmentInfos.getUserData().get(transferableDataKey1));
    assertEquals(transferableDataValue2, segmentInfos.getUserData().get(transferableDataKey2));

    // When we commit again without commit data in the command.
    commitCmd = new CommitUpdateCommand(req, false);
    req.getCore().getUpdateHandler().commit(commitCmd);

    // Then the empty commit is persisted and transfers commit-transferable data
    // from the previous commit.
    segmentInfos = readLatestCommit(core);
    assertEquals(0, segmentInfos.size());
    assertNull(segmentInfos.getUserData().get(transferableDataKey1));
    assertEquals(transferableDataValue2, segmentInfos.getUserData().get(transferableDataKey2));
  }
}
