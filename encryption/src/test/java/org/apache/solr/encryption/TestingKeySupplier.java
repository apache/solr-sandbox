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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
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
  public static final byte[] KEY_BLOB_1 = "ABCDE".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_BLOB_2 = "BCDEF".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_BLOB_3 = "CDEFG".getBytes(StandardCharsets.UTF_8);
  private static final Map<String, byte[]> KEY_BLOBS = Map.of(KEY_ID_1, KEY_BLOB_1,
                                                              KEY_ID_2, KEY_BLOB_2,
                                                              KEY_ID_3, KEY_BLOB_3);
  public static final byte[] KEY_SECRET_1 = "12345678901234567890123456789012".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_SECRET_2 = "34567890123456789012345678901234".getBytes(StandardCharsets.UTF_8);
  public static final byte[] KEY_SECRET_3 = "78901234567890123456789012345678".getBytes(StandardCharsets.UTF_8);
  private static final Map<String, byte[]> KEY_SECRETS = Map.of(KEY_ID_1, KEY_SECRET_1,
                                                                KEY_ID_2, KEY_SECRET_2,
                                                                KEY_ID_3, KEY_SECRET_3);

  private static final String KEY_BLOB_PARAM = "keyBlob";

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
    // Simulate a call to a Key Management System, passing the params
    // (e.g. tenant id, key id, etc) and getting a key 'blob' which would
    // be a wrapped form (encrypted) of the key secret.

    byte[] keyBlob = KEY_BLOBS.get(keyId);
    // Verify the key id is known.
    if (keyBlob == null) {
      throw new NoSuchElementException("No key defined for " + keyId);
    }
    // Verify the cookie params.
    if (!TestingEncryptionRequestHandler.MOCK_COOKIE_PARAMS.equals(params)) {
      throw new IllegalStateException("Wrong cookie params provided = " + params);
    }
    Map<String, String> cookie = new HashMap<>(params);
    cookie.put(KEY_BLOB_PARAM, Base64.getEncoder().encodeToString(keyBlob));
    return cookie;
  }

  @Override
  public byte[] getKeySecret(String keyId, Function<String, Map<String, String>> cookieSupplier) {
    // Simulate a call to a Key Management System, passing the key cookie
    // (e.g. tenant id, key id, key blob, etc) and getting the cleartext key secret.
    // This key secret could be stored in a short-lived cache with a dedicated thread
    // for automatic key wiping and removal.

    byte[] secret = KEY_SECRETS.get(keyId);
    // Verify the key id is known.
    if (secret == null) {
      throw new NoSuchElementException("No key defined for " + keyId);
    }
    Map<String, String> cookie = cookieSupplier.apply(keyId);
    // Verify the key secret is equal to the expected one.
    String keyBlobString = cookie == null ? null : cookie.get(KEY_BLOB_PARAM);
    byte[] keyBlobBytes = keyBlobString == null ?
      null : Base64.getDecoder().decode(keyBlobString);
    byte[] expectedKeyBlob = KEY_BLOBS.get(keyId);
    if (keyBlobBytes != null && expectedKeyBlob != null && !Arrays.equals(keyBlobBytes, expectedKeyBlob)
      || (keyBlobBytes == null || expectedKeyBlob == null) && keyBlobBytes != expectedKeyBlob) {
      throw new IllegalStateException("Wrong cookie provided = " + cookie);
    }
    // Verify the other cookie params.
    Map<String, String> otherParams = new HashMap<>(cookie);
    otherParams.remove(KEY_BLOB_PARAM);
    if (!TestingEncryptionRequestHandler.MOCK_COOKIE_PARAMS.equals(otherParams)) {
      throw new IllegalStateException("Wrong cookie params provided = " + cookie);
    }
    return secret;
  }

  @Override
  public void close() {}

  /**
   * Supplies the {@link TestingKeySupplier} singleton.
   */
  public static class Factory implements KeySupplier.Factory {

    private static final KeySupplier SINGLETON = new TestingKeySupplier();

    @Override
    public void init(NamedList<?> args, CoreContainer coreContainer) {
      // Do nothing.
    }

    @Override
    public KeySupplier create() {
      return SINGLETON;
    }
  }
}
