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
package org.apache.solr.faiss;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assume.assumeTrue;

/**
 * Integration test for FAISS module using Solr's test framework.
 */
public class TestFaissIntegration extends SolrTestCaseJ4 {

  private static Random random;
  private static final int DATASET_SIZE = 100;
  private static final int DATASET_DIMENSION = 8;
  private static final int TOPK = 5;
  private static final String ID_FIELD = "id";
  private static final String VECTOR_FIELD1 = "vector_field1";
  private static final String VECTOR_FIELD2 = "vector_field2";
  private static final String SOLRCONFIG_XML = "solrconfig.xml";
  private static final String SCHEMA_XML = "schema.xml";
  private static final String COLLECTION = "collection1";
  private static final String CONF_DIR = COLLECTION + "/conf";

  @BeforeClass
  public static void beforeClass() throws Exception {
    boolean faissAvailable = false;
    try {
      System.loadLibrary("faiss_c");
      faissAvailable = true;
    } catch (UnsatisfiedLinkError e) {
      // FAISS library not available
    }
    assumeTrue("FAISS native library not available", faissAvailable);

    Path tmpSolrHome = createTempDir();
    Path tmpConfDir = FilterPath.unwrap(tmpSolrHome.resolve(CONF_DIR));
    Path testHomeConfDir = TEST_HOME().resolve(CONF_DIR);
    Files.createDirectories(tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve(SOLRCONFIG_XML), tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve(SCHEMA_XML), tmpConfDir);

    initCore(SOLRCONFIG_XML, SCHEMA_XML, tmpSolrHome);
    random = new Random(222);
  }

  @Test
  public void testFaissCodecIsLoaded() {
    SolrCore solrCore = h.getCore();
    SolrConfig config = solrCore.getSolrConfig();
    String codecFactory = config.get("codecFactory").attr("class");
    assertEquals(
        "Unexpected solrconfig codec factory",
        "org.apache.solr.faiss.FaissCodecFactory",
        codecFactory);
    assertEquals("Unexpected core codec", "Lucene103", solrCore.getCodec().getName());
    assertTrue("Codec should be FaissCodec", solrCore.getCodec() instanceof FaissCodec);
  }

  @Test
  public void testIndexAndSearch() throws IOException {
    SolrCore solrCore = h.getCore();
    for (int i = 0; i < DATASET_SIZE; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(ID_FIELD, String.valueOf(i));
      List<Float> vector1 = generateRandomVector(random, DATASET_DIMENSION);
      List<Float> vector2 = generateRandomVector(random, DATASET_DIMENSION);
      doc.addField(VECTOR_FIELD1, vector1);
      doc.addField(VECTOR_FIELD2, vector2);
      assertU(adoc(doc));
    }
    assertU(commit());

    final RefCounted<SolrIndexSearcher> refCountedSearcher = solrCore.getSearcher();
    IndexSearcher searcher = refCountedSearcher.get();

    float[] queryVector = generateRandomVectorArray(random, DATASET_DIMENSION);
    KnnFloatVectorQuery q1 =
        new KnnFloatVectorQuery(VECTOR_FIELD1, queryVector, TOPK);
    TopDocs results1 = searcher.search(q1, TOPK);

    assertTrue("Should return at least some results", results1.scoreDocs.length > 0);
    assertTrue("Should return at most TOPK results", results1.scoreDocs.length <= TOPK);

    refCountedSearcher.decref();
  }

  private static List<Float> generateRandomVector(Random random, int dimensions) {
    List<Float> vector = new java.util.ArrayList<>();
    for (int j = 0; j < dimensions; j++) {
      vector.add(random.nextFloat() * 100);
    }
    return vector;
  }

  private static float[] generateRandomVectorArray(Random random, int dimension) {
    List<Float> vector = generateRandomVector(random, dimension);
    float[] query = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      query[i] = vector.get(i);
    }
    return query;
  }
}

