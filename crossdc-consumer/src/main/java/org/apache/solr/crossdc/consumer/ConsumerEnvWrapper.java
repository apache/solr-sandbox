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
package org.apache.solr.crossdc.consumer;

import org.apache.solr.crossdc.common.ConfigProperty;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * Simple wrapper to allow setting Consumer's config properties from environment variables.
 */
public class ConsumerEnvWrapper {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void main(String[] args) throws Exception {
    log.info("Initializing...");
    Map<String, String> env = System.getenv();
    for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
      String value = env.get(configKey.getKey());
      if (value != null) {
        log.debug("- copy env to sysprop: {}={}", configKey.getKey(), value);
        System.setProperty(configKey.getKey(), value);
      }
    }
    Consumer.main(args);
  }
}