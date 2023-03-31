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
import org.apache.lucene.store.Directory;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;

import java.io.IOException;

/**
 * Utility method to access to commit data.
 */
public class CommitUtil {

  /**
   * Reads the latest commit of the given {@link SolrCore}.
   */
  public static SegmentInfos readLatestCommit(SolrCore core) throws IOException {
    DirectoryFactory directoryFactory = core.getDirectoryFactory();
    Directory indexDir = directoryFactory.get(core.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_NONE);
    try {
      return SegmentInfos.readLatestCommit(indexDir);
    } finally {
      directoryFactory.release(indexDir);
    }
  }
}
