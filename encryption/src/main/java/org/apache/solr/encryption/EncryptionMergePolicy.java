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

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.solr.encryption.EncryptionUtil.*;

/**
 * Encrypts each segment individually with a forced merge.
 * <p>
 * Delegates all methods, but intercepts
 * {@link #findForcedMerges(SegmentInfos, int, Map, MergeContext)}
 * and if the requested max segment count is {@link Integer#MAX_VALUE} (a trigger
 * which means "keep all segments"), then force-merges individually each segment
 * which is not encrypted with the latest active key id.
 */
public class EncryptionMergePolicy extends FilterMergePolicy {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public EncryptionMergePolicy(MergePolicy in) {
    super(in);
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
                                             int maxSegmentCount,
                                             Map<SegmentCommitInfo,Boolean> segmentsToMerge,
                                             MergeContext mergeContext) throws IOException {
    if (maxSegmentCount != Integer.MAX_VALUE) {
      return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
    }
    if (segmentInfos.size() == 0) {
      return null;
    }
    Directory dir = segmentInfos.info(0).info.dir;
    if (!(dir instanceof EncryptionDirectory)) {
      // This may happen if the DirectoryFactory configured is not the EncryptionDirectoryFactory,
      // but this is a misconfiguration. Let's log an error.
      log.error("{} is configured whereas {} is not set; check the DirectoryFactory configuration",
                getClass().getName(), EncryptionDirectoryFactory.class.getName());
      return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
    }
    String keyRef = getActiveKeyRefFromCommit(segmentInfos.getUserData());
    String activeKeyId = keyRef == null ? null : getKeyIdFromCommit(keyRef, segmentInfos.getUserData());
    EncryptionDirectory encryptionDir = (EncryptionDirectory) dir;
    // Make sure the EncryptionDirectory does not keep its cache for the commit user data.
    // It must read the latest commit user data to get the latest active key, so below the
    // segments with old key (to re-encrypt) are always accurate.
    encryptionDir.forceReadCommitUserData();
    List<SegmentCommitInfo> segmentsWithOldKeyId = encryptionDir.getSegmentsWithOldKeyId(segmentInfos, activeKeyId);
    if (segmentsWithOldKeyId.isEmpty()) {
      return null;
    }
    // The goal is to rewrite each segment encrypted with an old key, so that it is re-encrypted
    // with the latest active encryption key.
    // Create a MergeSpecification containing multiple OneMerge, a OneMerge for each segment.
    MergeSpecification spec = new MergeSpecification();
    for (SegmentCommitInfo segmentInfo : segmentsWithOldKeyId) {
      spec.add(new OneMerge(Collections.singletonList(segmentInfo)));
    }
    return spec;
  }

  MergePolicy getDelegate() {
    return in;
  }
}
