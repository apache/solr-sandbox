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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.encryption.crypto.LightAesCtrEncrypter;
import org.apache.solr.index.MergePolicyFactoryArgs;
import org.apache.solr.index.TieredMergePolicyFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_2;

/**
 * Tests {@link EncryptionMergePolicyFactory}.
 */
public class EncryptionMergePolicyFactoryTest extends LuceneTestCase {

  private final SolrResourceLoader resourceLoader = new SolrResourceLoader(createTempDir());

  /**
   * Verifies the merge policy factory loading from solrconfig.xml.
   */
  @Test
  public void testMergePolicyCreation() {
    MergePolicy mergePolicy = createMergePolicy();
    assertEquals(EncryptionMergePolicy.class, mergePolicy.getClass());
    MergePolicy delegateMP = ((EncryptionMergePolicy) mergePolicy).getDelegate();
    assertEquals(TieredMergePolicy.class, delegateMP.getClass());
    TieredMergePolicy tieredMP = (TieredMergePolicy) delegateMP;
    assertEquals(5, tieredMP.getMaxMergeAtOnce());
  }

  private MergePolicy createMergePolicy() {
    final MergePolicyFactoryArgs args = new MergePolicyFactoryArgs();
    String prefix = "delegate";
    args.add("wrapped.prefix", prefix);
    args.add(prefix + ".class", TieredMergePolicyFactory.class.getName());
    args.add(prefix + ".maxMergeAtOnce", 5);
    return new EncryptionMergePolicyFactory(resourceLoader, args, null).getMergePolicy();
  }

  /**
   * Verifies that each segment is re-encrypted individually
   * (not requiring an optimized commit to merge into a single segment).
   */
  @Test
  public void testSegmentReencryption() throws Exception {
    KeySupplier keySupplier = new TestingKeySupplier.Factory().create();
    try (Directory dir = new EncryptionDirectory(new MMapDirectory(createTempDir(), FSLockFactory.getDefault()),
                                                 LightAesCtrEncrypter.FACTORY,
                                                 keySupplier)) {
      IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
      iwc.setMergeScheduler(new ConcurrentMergeScheduler());
      iwc.setMergePolicy(createMergePolicy());
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {

        // Index 3 segments with encryption key id 1.
        commit(writer, keySupplier, KEY_ID_1);
        int numSegments = 3;
        for (int i = 0; i < numSegments; ++i) {
          writer.addDocument(new Document());
          commit(writer, keySupplier, KEY_ID_1);
        }
        Set<String> initialSegmentNames = readSegmentNames(dir);
        assertEquals(numSegments, initialSegmentNames.size());

        // Run a force merge with the special max num segments trigger.
        writer.forceMerge(Integer.MAX_VALUE);
        commit(writer, keySupplier, KEY_ID_1);
        // Verify no segments are merged because they are encrypted with
        // the latest active key id.
        assertEquals(initialSegmentNames, readSegmentNames(dir));

        // Set the latest encryption key id 2.
        commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);

        // Run a force merge with any non-special max num segments.
        writer.forceMerge(10);
        commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
        // Verify no segments are merged.
        assertEquals(initialSegmentNames, readSegmentNames(dir));

        // Run a force merge with the special max num segments trigger.
        writer.forceMerge(Integer.MAX_VALUE);
        commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
        // Verify each segment has been rewritten.
        Set<String> segmentNames = readSegmentNames(dir);
        assertEquals(initialSegmentNames.size(), segmentNames.size());
        assertNotEquals(initialSegmentNames, segmentNames);
        segmentNames.retainAll(initialSegmentNames);
        assertTrue(segmentNames.isEmpty());
      }
    }
  }

  private void commit(IndexWriter writer, KeySupplier keySupplier, String... keyIds) throws IOException {
    Map<String, String> commitData = new HashMap<>();
    for (String keyId : keyIds) {
      EncryptionUtil.setNewActiveKeyIdInCommit(keyId, keySupplier.getKeyCookie(keyId, null), commitData);
    }
    writer.setLiveCommitData(commitData.entrySet());
    writer.commit();
  }

  private Set<String> readSegmentNames(Directory dir) throws IOException {
    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(dir);
    return segmentInfos.asList().stream().map(sci -> sci.info.name).collect(Collectors.toSet());
  }
}
