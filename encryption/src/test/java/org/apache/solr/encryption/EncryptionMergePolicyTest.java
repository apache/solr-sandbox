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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.apache.solr.encryption.crypto.LightAesCtrEncrypter;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.encryption.TestingEncryptionRequestHandler.MOCK_COOKIE_PARAMS;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_1;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_ID_2;

/**
 * Tests {@link EncryptionMergePolicy}.
 */
public class EncryptionMergePolicyTest extends LuceneTestCase {

    private final Path tempDir = createTempDir();
    private final KeySupplier keySupplier = new TestingKeySupplier.Factory().create();
    private final AesCtrEncrypterFactory encrypterFactory = LightAesCtrEncrypter.FACTORY;

    @Test
    public void testNoReencryptionWhenNoKeyChange() throws Exception {
        try (Directory dir = new EncryptionDirectory(
                new MMapDirectory(tempDir, FSLockFactory.getDefault()),
                encrypterFactory,
                keySupplier)) {
            
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            iwc.setMergePolicy(createMergePolicy());
            
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create initial segments with KEY_ID_1.
                commit(writer, keySupplier, KEY_ID_1);
                int numSegments = 3;
                for (int i = 0; i < numSegments; ++i) {
                    writer.addDocument(new Document());
                    commit(writer, keySupplier, KEY_ID_1);
                }
                
                Set<String> initialSegmentNames = readSegmentNames(dir);
                assertEquals(numSegments, initialSegmentNames.size());

                // Force merge with MAX_VALUE should not trigger reencryption.
                writer.forceMerge(Integer.MAX_VALUE);
                commit(writer, keySupplier, KEY_ID_1);
                
                // Verify segments remain unchanged.
                assertEquals(initialSegmentNames, readSegmentNames(dir));
            }
        }
    }

    @Test
    public void testReencryptionWithKeyChange() throws Exception {
        try (Directory dir = new EncryptionDirectory(
                new MMapDirectory(tempDir, FSLockFactory.getDefault()),
                encrypterFactory,
                keySupplier)) {
            
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            iwc.setMergePolicy(createMergePolicy());
            
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create initial segments with KEY_ID_1.
                commit(writer, keySupplier, KEY_ID_1);
                int numSegments = 3;
                for (int i = 0; i < numSegments; ++i) {
                    writer.addDocument(new Document());
                    commit(writer, keySupplier, KEY_ID_1);
                }
                
                Set<String> initialSegmentNames = readSegmentNames(dir);
                assertEquals(numSegments, initialSegmentNames.size());

                // Change active key to KEY_ID_2.
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);

                // Force merge with MAX_VALUE should trigger reencryption.
                writer.forceMerge(Integer.MAX_VALUE);
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
                
                // Verify all segments have been rewritten.
                Set<String> newSegmentNames = readSegmentNames(dir);
                assertEquals(initialSegmentNames.size(), newSegmentNames.size());
                assertNotEquals(initialSegmentNames, newSegmentNames);
                newSegmentNames.retainAll(initialSegmentNames);
                assertTrue(newSegmentNames.isEmpty());
            }
        }
    }

    @Test
    public void testNoReencryptionWithNonMaxValueForceMerge() throws Exception {
        try (Directory dir = new EncryptionDirectory(
                new MMapDirectory(tempDir, FSLockFactory.getDefault()),
                encrypterFactory,
                keySupplier)) {
            
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            iwc.setMergePolicy(createMergePolicy());
            
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create initial segments with KEY_ID_1.
                commit(writer, keySupplier, KEY_ID_1);
                int numSegments = 3;
                for (int i = 0; i < numSegments; ++i) {
                    writer.addDocument(new Document());
                    commit(writer, keySupplier, KEY_ID_1);
                }
                
                Set<String> initialSegmentNames = readSegmentNames(dir);
                assertEquals(numSegments, initialSegmentNames.size());

                // Change active key to KEY_ID_2.
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);

                // Force merge with non-MAX_VALUE should not trigger reencryption.
                writer.forceMerge(10);
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
                
                // Verify segments remain unchanged.
                assertEquals(initialSegmentNames, readSegmentNames(dir));
            }
        }
    }

    @Test
    public void testEmptyIndex() throws Exception {
        try (Directory dir = new EncryptionDirectory(
                new MMapDirectory(tempDir, FSLockFactory.getDefault()),
                encrypterFactory,
                keySupplier)) {
            
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            iwc.setMergePolicy(createMergePolicy());
            
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create empty index with KEY_ID_1.
                commit(writer, keySupplier, KEY_ID_1);
                
                // Change active key to KEY_ID_2.
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);

                // Force merge with MAX_VALUE should not trigger any merges.
                writer.forceMerge(Integer.MAX_VALUE);
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
                
                // Verify no segments exist.
                assertEquals(0, readSegmentNames(dir).size());
            }
        }
    }

    @Test
    public void testPartiallyEncryptedSegments() throws Exception {
        try (Directory dir = new EncryptionDirectory(
                new MMapDirectory(tempDir, FSLockFactory.getDefault()),
                encrypterFactory,
                keySupplier)) {
            
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            iwc.setMergePolicy(createMergePolicy());
            
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create segments with mixed encryption states.
                commit(writer, keySupplier, KEY_ID_1);
                
                // Add some documents with KEY_ID_1.
                for (int i = 0; i < 3; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                    writer.addDocument(doc);
                    commit(writer, keySupplier, KEY_ID_1);
                }

                Set<String> key1SegmentNames = readSegmentNames(dir);
                assertEquals(3, key1SegmentNames.size());

                // Change to KEY_ID_2.
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
                
                // Add more documents with KEY_ID_2.
                for (int i = 3; i < 6; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                    writer.addDocument(doc);
                    commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
                }
                
                Set<String> key2NewSegmentNames = readSegmentNames(dir);
                key2NewSegmentNames.removeAll(key1SegmentNames);
                assertEquals(3, key2NewSegmentNames.size());

                // Force merge with MAX_VALUE should trigger reencryption of old segments.
                writer.forceMerge(Integer.MAX_VALUE);
                commit(writer, keySupplier, KEY_ID_1, KEY_ID_2);
                
                // Verify only and all key1 segments have been rewritten.
                Set<String> finalSegmentNames = readSegmentNames(dir);
                assertEquals(6, finalSegmentNames.size());
                assertTrue(finalSegmentNames.containsAll(key2NewSegmentNames));
                assertTrue(finalSegmentNames.stream().noneMatch(key1SegmentNames::contains));
            }
        }
    }

    private MergePolicy createMergePolicy() {
        return new EncryptionMergePolicy(new TieredMergePolicy());
    }

    private void commit(IndexWriter writer, KeySupplier keySupplier, String... keyIds) throws IOException {
        Map<String, String> commitData = new HashMap<>();
        for (String keyId : keyIds) {
            EncryptionUtil.setNewActiveKeyIdInCommit(keyId, keySupplier.getKeyCookie(keyId, MOCK_COOKIE_PARAMS), commitData);
        }
        writer.setLiveCommitData(commitData.entrySet());
        writer.commit();
    }

    private Set<String> readSegmentNames(Directory dir) throws IOException {
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(dir);
        return segmentInfos.asList().stream()
                .map(sci -> sci.info.name)
                .collect(Collectors.toSet());
    }
} 