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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.junit.Assert;

import java.nio.file.Path;

/**
 * Utility methods for encryption tests.
 */
public class TestUtil {

  private final CloudSolrClient solrClient;
  private final String collectionName;
  private int docId;

  public TestUtil(CloudSolrClient solrClient, String collectionName) {
    this.solrClient = solrClient;
    this.collectionName = collectionName;
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
   * Adds one doc per provided text, and commits.
   */
  public void indexDocsAndCommit(String... texts) throws Exception {
    for (String text : texts) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Integer.toString(docId++));
      doc.addField("text", text);
      solrClient.add(doc);
    }
    solrClient.commit(collectionName);
  }

  /**
   * Verifies that the provided query returns the expected number of results.
   */
  public void assertQueryReturns(String query, int expectedNumResults) throws Exception {
    QueryResponse response = solrClient.query(new SolrQuery(query));
    Assert.assertEquals(expectedNumResults, response.getResults().size());
  }

  /**
   * Reloads the leader replica core of the first shard of the collection.
   */
  public void reloadCore() throws Exception {
    try {
      DocCollection collection = solrClient.getClusterState().getCollection(collectionName);
      Slice slice = collection.getSlices().iterator().next();
      CoreAdminRequest.reloadCore(slice.getLeader().core, solrClient);
    } catch (SolrException e) {
      throw new CoreReloadException("The index cannot be reloaded. There is probably an issue with the encryption key ids.", e);
    }
  }

  /**
   * Verifies that {@link #reloadCore()} fails.
   */
  public void assertCannotReloadCore() throws Exception {
    try {
      reloadCore();
      Assert.fail("Core reloaded whereas it was not expected to be possible");
    } catch (CoreReloadException e) {
      // Expected.
    }
  }

  private static class CoreReloadException extends Exception {
    CoreReloadException(String msg, SolrException cause) {
      super(msg, cause);
    }
  }
}
