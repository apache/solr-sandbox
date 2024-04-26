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

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SplitIndexCommand;
import org.apache.solr.update.UpdateHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.encryption.CommitUtil.readLatestCommit;

/**
 * Extends {@link org.apache.solr.update.DirectUpdateHandler2} and adds the capability
 * to transfer some transferable user data from the previous commit to the next one.
 */
public class EncryptionUpdateHandler extends DirectUpdateHandler2 {

  /**
   * Parameter prefix to state that this parameter should be transferred from a commit
   * user data to the next commit user data automatically.
   */
  public static final String TRANSFERABLE_COMMIT_DATA = "#transfer.";

  public EncryptionUpdateHandler(SolrCore core) {
    super(core);
  }

  public EncryptionUpdateHandler(SolrCore core, UpdateHandler updateHandler) {
    super(core, updateHandler);
  }

  /**
   * Transfers commit-transferable data just before the effective call to {@link IndexWriter#commit()}.
   * This method is atomically protected with the commit lock.
   */
  @Override
  protected boolean shouldCommit(CommitUpdateCommand cmd, IndexWriter writer) throws IOException {
    if (!super.shouldCommit(cmd, writer)) {
      return false;
    }
    cmd.commitData = transferCommitData(cmd.commitData);
    return true;
  }

  private Map<String, String> transferCommitData(Map<String, String> commitData) throws IOException {
    // Two cases:
    // - If commitData is null, then transfer all the latest commit transferable
    //   data to the current commit.
    // - If commitData is not null, nothing is transferred. It is the caller
    //   responsibility to include all the required user data from the latest commit.
    //   That way, the caller can remove some entries.
    if (commitData == null) {
      Map<String, String> latestCommitData = readLatestCommit(core).getUserData();
      Map<String, String> newCommitData = null;
      for (Map.Entry<String, String> latestCommitEntry : latestCommitData.entrySet()) {
        if (latestCommitEntry.getKey().startsWith(TRANSFERABLE_COMMIT_DATA)) {
          if (newCommitData == null) {
            newCommitData = new HashMap<>();
          }
          newCommitData.put(latestCommitEntry.getKey(), latestCommitEntry.getValue());
        }
      }
      return newCommitData;
    }
    return commitData;
  }

  @Override
  public void split(SplitIndexCommand cmd) throws IOException {
    // Transfer the parent core commit data to the sub-shard cores.
    cmd.commitData = transferCommitData(cmd.commitData);
    super.split(cmd);
  }
}
