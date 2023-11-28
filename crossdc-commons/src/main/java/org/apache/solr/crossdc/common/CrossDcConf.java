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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public abstract class CrossDcConf {
    public static final String CROSSDC_PROPERTIES = "/crossdc.properties";
    public static final String ZK_CROSSDC_PROPS_PATH = "zkCrossDcPropsPath";
    public static final String EXPAND_DBQ = "expandDbq";

    public enum ExpandDbq {
        NONE,
        EXPAND;

        private static final Map<String, ExpandDbq> valueMap = new HashMap<>();
        static {
            for (ExpandDbq value : values()) {
                valueMap.put(value.name().toUpperCase(Locale.ROOT), value);
            }
        }

        public static ExpandDbq getOrDefault(String strValue, ExpandDbq defaultValue) {
            if (strValue == null || strValue.isBlank()) {
                return defaultValue;
            }
            ExpandDbq value = valueMap.get(strValue.toUpperCase(Locale.ROOT));
            return value != null ? value : defaultValue;
        }
    }
}
