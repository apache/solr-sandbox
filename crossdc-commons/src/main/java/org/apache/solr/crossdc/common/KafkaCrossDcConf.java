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

public class KafkaCrossDcConf extends CrossDcConf {
    private final String topicName;

    private final String groupId;
    private final boolean enableDataEncryption;
    private final String bootstrapServers;
    private long slowSubmitThresholdInMillis;
    private int numOfRetries = 5;
    private final String solrZkConnectString;

    private final int maxPollRecords;

    public KafkaCrossDcConf(String bootstrapServers, String topicName, String groupId, int maxPollRecords, boolean enableDataEncryption, String solrZkConnectString) {
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.enableDataEncryption = enableDataEncryption;
        this.solrZkConnectString = solrZkConnectString;
        this.groupId = groupId;
        this.maxPollRecords = maxPollRecords;
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

    @Override
    public String toString() {
        return String.format("KafkaCrossDcConf{" +
                "topicName='%s', " +
                "groupId='%s', " +
                "enableDataEncryption='%b', " +
                "bootstrapServers='%s', " +
                "slowSubmitThresholdInMillis='%d', " +
                "numOfRetries='%d', " +
                "solrZkConnectString='%s'}",
                topicName, groupId, enableDataEncryption, bootstrapServers,
                slowSubmitThresholdInMillis, numOfRetries, solrZkConnectString);
    }
}
