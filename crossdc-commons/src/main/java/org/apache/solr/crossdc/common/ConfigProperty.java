package org.apache.solr.crossdc.common;

import java.util.Map;
import java.util.Properties;

public class ConfigProperty {

  private final String key;
  private final String defaultValue;

  private boolean required = false;

  public ConfigProperty(String key, String defaultValue, boolean required) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.required = required;
  }

  public ConfigProperty(String key, String defaultValue) {
    this.key = key;
    this.defaultValue = defaultValue;
  }

  public ConfigProperty(String key) {
    this.key = key;
    this.defaultValue = null;
  }

  public String getKey() {
    return key;
  }

  public boolean isRequired() {
    return required;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String getValue(Map properties) {
    String val = (String) properties.get(key);
    if (val == null) {
     return defaultValue;
    }
    return val;
  }

  public Integer getValueAsInt(Map properties) {
    String value = (String) properties.get(key);
    if (value != null) {
      return Integer.parseInt(value);
    }
    if (defaultValue == null) {
      return null;
    }
    return Integer.parseInt(defaultValue);
  }

  public Boolean getValueAsBoolean(Map properties) {
    String value = (String) properties.get(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return Boolean.parseBoolean(defaultValue);
  }
}
