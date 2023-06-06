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

import org.apache.lucene.index.IndexFileNames;
import org.apache.solr.common.params.SolrParams;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

/**
 * Mocked implementation of {@link KeySupplier}.
 */
public class TestingKeySupplier implements KeySupplier {

  public static final String KEY_ID_1 = "mock1";
  public static final String KEY_ID_2 = "mock2";
  public static final String KEY_ID_3 = "mock3";
  public static final byte[] KEY_COOKIE_1 = "ABCDE".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_COOKIE_2 = "BCDEF".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_COOKIE_3 = "CDEFG".getBytes(StandardCharsets.UTF_8);
  private static final Map<String, byte[]> MOCK_COOKIES = Map.of(KEY_ID_1, KEY_COOKIE_1, KEY_ID_2, KEY_COOKIE_2, KEY_ID_3, KEY_COOKIE_3);
  public static final byte[] KEY_SECRET_1 = "12345678901234567890123456789012".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_SECRET_2 = "34567890123456789012345678901234".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_SECRET_3 = "78901234567890123456789012345678".getBytes(StandardCharsets.UTF_8);
  private static final Map<String, byte[]> MOCK_KEYS = Map.of(KEY_ID_1, KEY_SECRET_1, KEY_ID_2, KEY_SECRET_2, KEY_ID_3, KEY_SECRET_3);

  /**
   * File name extensions/suffixes that do NOT need to be encrypted because it lacks user/external data.
   * Other files should be encrypted.
   * There is some human judgement here as some files may contain vague clues as to the shape of the data.
   */
  private static final Set<String> CLEARTEXT_EXTENSIONS = Set.of(
    "doc",    // Document number, frequencies, and skip data
    "pos",    // Positions
    "pay",    // Payloads and offsets
    "dvm",    // Doc values metadata
    "fdm",    // Stored fields metadata
    "fdx",    // Stored fields index
    "nvd",    // Norms data
    "nvm",    // Norms metadata
    "fnm",    // Field Infos
    "si",     // Segment Infos
    "cfe"     // Compound file entries
    );
  // Extensions known to contain sensitive user data, and thus that need to be encrypted:
  // tip    - BlockTree terms index (FST)
  // tim    - BlockTree terms
  // tmd    - BlockTree metadata (contains first and last term)
  // fdt    - Stored fields data
  // dvd    - Doc values data
  // ustd   - UniformSplit index (FST)
  // ustb   - UniformSplit terms (including metadata)
  // cfs    - Compound file (contains all the above files data)

  // Cleartext temporary files:
  private static final String TMP_EXTENSION = "tmp";
  private static final String TMP_DOC_IDS = "-doc_ids"; // FieldsIndexWriter
  private static final String TMP_FILE_POINTERS = "file_pointers"; // FieldsIndexWriter

  private TestingKeySupplier() {}

  @Override
  public boolean shouldEncrypt(String fileName) {
    String extension = IndexFileNames.getExtension(fileName);
    if (extension == null) {
      // segments and pending_segments are never passed as parameter of this method.
      assert !fileName.startsWith(IndexFileNames.SEGMENTS) && !fileName.startsWith(IndexFileNames.PENDING_SEGMENTS);
    } else if (CLEARTEXT_EXTENSIONS.contains(extension)) {
      // The file extension tells us it does not need to be encrypted.
      return false;
    } else if (extension.equals(TMP_EXTENSION)) {
      // We know some tmp files do not need to be encrypted.
      int tmpCounterIndex = fileName.lastIndexOf('_');
      assert tmpCounterIndex != -1;
      if (endsWith(fileName, TMP_DOC_IDS, tmpCounterIndex)
      || endsWith(fileName, TMP_FILE_POINTERS, tmpCounterIndex)) {
        return false;
      }
    }
    // By default, all other files should be encrypted.
    return true;
  }

  private static boolean endsWith(String s, String suffix, int endIndex) {
    // Inspired from JDK String where endsWith calls startsWith.
    // Here we look for [suffix] from index [endIndex - suffix.length()].
    // This is equivalent to
    // s.substring(0, endIndex).endsWith(suffix)
    // without creating a substring.
    return s.startsWith(suffix, endIndex - suffix.length());
  }

  @Override
  public Map<String, String> getKeyCookie(String keyId, Map<String, String> params) {
    byte[] wrappedKeySecret = MOCK_COOKIES.get(keyId);
    if (wrappedKeySecret == null) {
      throw new NoSuchElementException("No key defined for " + keyId);
    }
    return Map.of("wrappedSecret", Base64.getEncoder().encodeToString(wrappedKeySecret));
  }

  @Override
  public byte[] getKeySecret(String keyId, Function<String, Map<String, String>> cookieSupplier) {
    byte[] secret = MOCK_KEYS.get(keyId);
    if (secret == null) {
      throw new NoSuchElementException("No key defined for " + keyId);
    }
    Map<String, String> cookie = cookieSupplier.apply(keyId);
    String wrappedKeySecretAsString = cookie == null ? null : cookie.get("wrappedSecret");
    byte[] wrappedKeySecret = wrappedKeySecretAsString == null ?
      null : Base64.getDecoder().decode(wrappedKeySecretAsString);
    byte[] expectedWrappedKeySecret = MOCK_COOKIES.get(keyId);
    if (wrappedKeySecret != null && expectedWrappedKeySecret != null && !Arrays.equals(wrappedKeySecret, expectedWrappedKeySecret)
      || (wrappedKeySecret == null || expectedWrappedKeySecret == null) && wrappedKeySecret != expectedWrappedKeySecret) {
      throw new IllegalStateException("Wrong cookie provided");
    }
    return secret;
  }

  /**
   * Supplies the {@link TestingKeySupplier} singleton.
   */
  public static class Factory implements KeySupplier.Factory {

    private static final KeySupplier SINGLETON = new TestingKeySupplier();

    @Override
    public void init(SolrParams params) {
      // Do nothing.
    }

    @Override
    public KeySupplier create() {
      return SINGLETON;
    }
  }
}
