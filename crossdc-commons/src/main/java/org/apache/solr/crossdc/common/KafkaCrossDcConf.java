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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

public class KafkaCrossDcConf extends CrossDcConf {

  public static final String DEFAULT_BATCH_SIZE_BYTES = "2097152";
  public static final String DEFAULT_BUFFER_MEMORY_BYTES = "536870912";
  public static final String DEFAULT_LINGER_MS = "30";
  public static final String DEFAULT_REQUEST_TIMEOUT = "60000";
  public static final String DEFAULT_MAX_REQUEST_SIZE = "5242880";
  public static final String DEFAULT_ENABLE_DATA_COMPRESSION = "none";
  public static final String DEFAULT_SLOW_SEND_THRESHOLD= "1000";
  public static final String DEFAULT_NUM_RETRIES = null; // by default, we control retries with DELIVERY_TIMEOUT_MS_DOC
  private static final String DEFAULT_RETRY_BACKOFF_MS = "500";

  private static final String DEFAULT_DELIVERY_TIMEOUT_MS = "120000";

  public static final String DEFAULT_MAX_POLL_RECORDS = "500"; // same default as Kafka

  private static final String DEFAULT_FETCH_MIN_BYTES = "512000";
  private static final String DEFAULT_FETCH_MAX_WAIT_MS = "1000"; // Kafka default is 500

  public static final String DEFAULT_FETCH_MAX_BYTES = "100663296";

  public static final String DEFAULT_MAX_PARTITION_FETCH_BYTES = "33554432";

  public static final String DEFAULT_PORT = "8090";

  private static final String DEFAULT_GROUP_ID = "SolrCrossDCConsumer";


  public static final String TOPIC_NAME = "topicName";

  public static final String BOOTSTRAP_SERVERS = "bootstrapServers";

  public static final String BATCH_SIZE_BYTES = "batchSizeBytes";

  public static final String BUFFER_MEMORY_BYTES = "bufferMemoryBytes";

  public static final String LINGER_MS = "lingerMs";

  public static final String REQUEST_TIMEOUT_MS = "requestTimeoutMS";

  public static final String MAX_REQUEST_SIZE_BYTES = "maxRequestSizeBytes";

  public static final String ENABLE_DATA_COMPRESSION = "enableDataCompression";

  public static final String SLOW_SUBMIT_THRESHOLD_MS = "slowSubmitThresholdMs";

  public static final String NUM_RETRIES = "numRetries";

  public static final String RETRY_BACKOFF_MS = "retryBackoffMs";

  public static final String DELIVERY_TIMEOUT_MS = "retryBackoffMs";

  public static final String FETCH_MIN_BYTES = "fetchMinBytes";

  public static final String FETCH_MAX_WAIT_MS = "fetchMaxWaitMS";

  public static final String MAX_POLL_RECORDS = "maxPollRecords";

  public static final String FETCH_MAX_BYTES = "fetchMaxBytes";

  public static final String MAX_PARTITION_FETCH_BYTES = "maxPartitionFetchBytes";

  public static final String ZK_CONNECT_STRING = "zkConnectString";




  public static final List<ConfigProperty> CONFIG_PROPERTIES;
  private static final HashMap<String, ConfigProperty> CONFIG_PROPERTIES_MAP;

  public static final String PORT = "port";

  public static final String GROUP_ID = "groupId";



  static {
    CONFIG_PROPERTIES =
        List.of(new ConfigProperty(TOPIC_NAME), new ConfigProperty(BOOTSTRAP_SERVERS),
            new ConfigProperty(BATCH_SIZE_BYTES, DEFAULT_BATCH_SIZE_BYTES),
            new ConfigProperty(BUFFER_MEMORY_BYTES, DEFAULT_BUFFER_MEMORY_BYTES),
            new ConfigProperty(LINGER_MS, DEFAULT_LINGER_MS),
            new ConfigProperty(REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT),
            new ConfigProperty(MAX_REQUEST_SIZE_BYTES, DEFAULT_MAX_REQUEST_SIZE),
            new ConfigProperty(ENABLE_DATA_COMPRESSION, DEFAULT_ENABLE_DATA_COMPRESSION),
            new ConfigProperty(SLOW_SUBMIT_THRESHOLD_MS, DEFAULT_SLOW_SEND_THRESHOLD),
            new ConfigProperty(NUM_RETRIES, DEFAULT_NUM_RETRIES),
            new ConfigProperty(RETRY_BACKOFF_MS, DEFAULT_RETRY_BACKOFF_MS),
            new ConfigProperty(DELIVERY_TIMEOUT_MS, DEFAULT_DELIVERY_TIMEOUT_MS),

            // Consumer only zkConnectString
            new ConfigProperty(ZK_CONNECT_STRING, null),
            new ConfigProperty(FETCH_MIN_BYTES, DEFAULT_FETCH_MIN_BYTES),
            new ConfigProperty(FETCH_MAX_BYTES, DEFAULT_FETCH_MAX_BYTES),
            new ConfigProperty(FETCH_MAX_WAIT_MS, DEFAULT_FETCH_MAX_WAIT_MS),

            new ConfigProperty(MAX_PARTITION_FETCH_BYTES, DEFAULT_MAX_PARTITION_FETCH_BYTES),
            new ConfigProperty(MAX_POLL_RECORDS, DEFAULT_MAX_POLL_RECORDS),
            new ConfigProperty(PORT, DEFAULT_PORT),
            new ConfigProperty(GROUP_ID, DEFAULT_GROUP_ID)
            );



    CONFIG_PROPERTIES_MAP = new HashMap<String, ConfigProperty>(CONFIG_PROPERTIES.size());
    for (ConfigProperty prop : CONFIG_PROPERTIES) {
      CONFIG_PROPERTIES_MAP.put(prop.getKey(), prop);
    }
  }

  private final Map<String, String> properties;

  public KafkaCrossDcConf(Map<String, String> properties) {
    List<String> nullValueKeys = new ArrayList<String>();
    properties.forEach((k, v) -> {
      if (v == null) {
        nullValueKeys.add(k);
      }
    });
    nullValueKeys.forEach(properties::remove);
    this.properties = properties;
  }

  public String get(String property) {
    return CONFIG_PROPERTIES_MAP.get(property).getValue(properties);
  }

  public Integer getInt(String property) {
    ConfigProperty prop = CONFIG_PROPERTIES_MAP.get(property);
    if (prop == null) {
      throw new IllegalArgumentException("Property not found key=" + property);
    }
    return prop.getValueAsInt(properties);
  }

  public Boolean getBool(String property) {
    ConfigProperty prop = CONFIG_PROPERTIES_MAP.get(property);
    if (prop == null) {
      throw new IllegalArgumentException("Property not found key=" + property);
    }
    return prop.getValueAsBoolean(properties);
  }
  
  public Properties getAdditionalProperties() {
    Properties additional = new Properties();
    additional.putAll(properties);
    for (ConfigProperty configProperty : CONFIG_PROPERTIES) {
      additional.remove(configProperty.getKey());
    }
    Map<String, Object> integerProperties = new HashMap<>();
    additional.forEach((k, v) -> {
      try {
        int intVal = Integer.parseInt((String) v);
        integerProperties.put(k.toString(), intVal);
      } catch (NumberFormatException ignored) {

      }
    });
    additional.putAll(integerProperties);
    return additional;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder(128);
    for (ConfigProperty configProperty : CONFIG_PROPERTIES) {
      sb.append(configProperty.getKey()).append("=")
          .append(properties.get(configProperty.getKey())).append(",");
    }
    sb.setLength(sb.length() - 1);

    return "KafkaCrossDcConf{" + sb.toString() + "}";
  }

}
