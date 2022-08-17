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
package org.apache.solr.crossdc.common;

import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaCrossDcConf extends CrossDcConf {

    public static final String DEFAULT_BATCH_SIZE_BYTES = "512000";
    public static final String DEFAULT_BUFFER_MEMORY_BYTES = "268435456";
    public static final String DEFAULT_LINGER_MS = "30";
    public static final String DEFAULT_REQUEST_TIMEOUT = "60000";

    private final String topicName;

    private final String groupId;
    private final boolean enableDataEncryption;
    private final String bootstrapServers;
    private long slowSubmitThresholdInMillis = 1000;
    private int numOfRetries = 5;
    private final String solrZkConnectString;

    private final int maxPollRecords;

    private final int batchSizeBytes;
    private final int bufferMemoryBytes;
    private final int lingerMs;
    private final int requestTimeout;

  private final int fetchMinBytes;

  private final int fetchMaxWaitMS;

  public KafkaCrossDcConf(String bootstrapServers, String topicName, String groupId, int maxPollRecords, int batchSizeBytes, int bufferMemoryBytes, int lingerMs, int requestTimeout,
      int fetchMinBytes, int fetchMaxWaitMS, boolean enableDataEncryption, String solrZkConnectString) {
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.enableDataEncryption = enableDataEncryption;
        this.solrZkConnectString = solrZkConnectString;
        this.groupId = groupId;
        this.maxPollRecords = maxPollRecords;
        this.batchSizeBytes = batchSizeBytes;
        this.bufferMemoryBytes = bufferMemoryBytes;
        this.lingerMs = lingerMs;
        this.requestTimeout = requestTimeout;
        this.fetchMinBytes = fetchMinBytes;
        this.fetchMaxWaitMS = fetchMaxWaitMS;
    }
    public String getTopicName() {
        return topicName;
    }

    public boolean getEnableDataEncryption() { return enableDataEncryption; }

    public long getSlowSubmitThresholdInMillis() {
        return slowSubmitThresholdInMillis;
    }

    public void setSlowSubmitThresholdInMillis(long slowSubmitThresholdInMillis) {
        this.slowSubmitThresholdInMillis = slowSubmitThresholdInMillis;
    }

    public int getNumOfRetries() {
        return numOfRetries;
    }

    public String getSolrZkConnectString() {
        return solrZkConnectString;
    }

    @Override
    public String getClusterName() {
        return null;
    }

    public String getGroupId() {
        return groupId;
  }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public String getBootStrapServers() {
        return bootstrapServers;
    }

    public int getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public int getBufferMemoryBytes() {
        return bufferMemoryBytes;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getFetchMinBytes() {
      return fetchMinBytes;
    }

    public int getFetchMaxWaitMS() {
      return fetchMaxWaitMS;
    }

    @Override
    public String toString() {
        return String.format("KafkaCrossDcConf{" +
                "topicName='%s', " +
                "groupId='%s', " +
                "enableDataEncryption='%b', " +
                "bootstrapServers='%s', " +
                "slowSubmitThresholdInMillis='%d', " +
                "numOfRetries='%d', " +
                "batchSizeBytes='%d', " +
                "bufferMemoryBytes='%d', " +
                "lingerMs='%d', " +
                "requestTimeout='%d', " +
                "fetchMinBytes='%d', " +
                "fetchMaxWaitMS='%d', " +
                "solrZkConnectString='%s'}",
                topicName, groupId, enableDataEncryption, bootstrapServers,
                slowSubmitThresholdInMillis, numOfRetries, batchSizeBytes,
                bufferMemoryBytes, lingerMs, requestTimeout, fetchMinBytes, fetchMaxWaitMS, solrZkConnectString);
    }
}
