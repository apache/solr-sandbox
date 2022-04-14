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
    private final boolean enableDataEncryption;
    private long slowSubmitThresholdInMillis;
    private int numOfRetries = 5;
    private final String solrZkConnectString;


    public KafkaCrossDcConf(String topicName, boolean enableDataEncryption, String solrZkConnectString) {
        this.topicName = topicName;
        this.enableDataEncryption = enableDataEncryption;
        this.solrZkConnectString = solrZkConnectString;
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
}
