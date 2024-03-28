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

import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Constants and methods for encryption.
 */
public class EncryptionUtil {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Log prefix for encryption, to ease log search. */
  public static final String ENCRYPTION_LOG_PREFIX = "Encryption:";

  /**
   * Crypto parameter prefix, in the commit user data.
   * It includes the {@link EncryptionUpdateHandler#TRANSFERABLE_COMMIT_DATA} prefix to be transferred from a
   * commit to the next one automatically.
   */
  public static final String COMMIT_CRYPTO = EncryptionUpdateHandler.TRANSFERABLE_COMMIT_DATA + "crypto.";

  /**
   * Active encryption key ref parameter, in the commit user data.
   * If this parameter is not present, it means the index is in cleartext and has never been encrypted.
   * This parameter value is the reference number of the key in the {@link #COMMIT_KEY_ID} and
   * {@link #COMMIT_KEY_COOKIE} mappings.
   */
  public static final String COMMIT_ACTIVE_KEY = COMMIT_CRYPTO + "active";

  /**
   * Commit user data parameter that maps a key reference number to its corresponding key id.
   * The complete parameter name is the concatenation of this parameter prefix and the key reference number.
   */
  public static final String COMMIT_KEY_ID = COMMIT_CRYPTO + "id.";

  /**
   * Commit user data parameter that maps a key reference number to its corresponding crypto cookie.
   * The complete parameter name is the concatenation of this parameter prefix and the key reference number.
   */
  public static final String COMMIT_KEY_COOKIE = COMMIT_CRYPTO + "cookie.";

  /**
   * Number of inactive key ids to keep when clearing the old inactive key ids.
   * @see #clearOldInactiveKeyIdsFromCommit
   */
  private static final int INACTIVE_KEY_IDS_TO_KEEP = 15;

  /**
   * Sets the new active encryption key id, and its optional cookie in the provided commit user data.
   * New index files will be encrypted using this new key.
   *
   * @param keyId          the new active encryption key id; must not be null.
   * @param keyCookie      the key-value pairs associated to the key id; may be null.
   * @param commitUserData read to retrieve the current active key ref, and then updated with the new
   *                       active key ref.
   */
  public static void setNewActiveKeyIdInCommit(String keyId,
                                               @Nullable Map<String, String> keyCookie,
                                               Map<String, String> commitUserData) {
    // Key references are integers stored as strings. They are ordered by the natural ordering of
    // integers. This method is the only location where key references are created. Outside, key
    // references are simply considered as strings, except clearOldInactiveKeyIdsFromCommit() which
    // sorts key references by the integer ordering.
    assert keyId != null;
    String oldKeyRef = getActiveKeyRefFromCommit(commitUserData);
    String newKeyRef = oldKeyRef == null ? "0" : Integer.toString(Integer.parseInt(oldKeyRef) + 1);
    commitUserData.put(COMMIT_ACTIVE_KEY, newKeyRef);
    commitUserData.put(COMMIT_KEY_ID + newKeyRef, keyId);
    if (keyCookie != null) {
      commitUserData.put(COMMIT_KEY_COOKIE + newKeyRef, Utils.toJSONString(keyCookie));
    }
  }

  /**
   * Removes the active encryption key id.
   * New index files will be cleartext.
   *
   * @param commitUserData updated to remove the active key ref.
   */
  public static void removeActiveKeyRefFromCommit(Map<String, String> commitUserData) {
    commitUserData.remove(COMMIT_ACTIVE_KEY);
  }

  /**
   * Gets the reference number of the currently active encryption key, from the provided commit user data.
   *
   * @return the reference number of the active encryption key; or null if none, which means cleartext.
   */
  @Nullable
  public static String getActiveKeyRefFromCommit(Map<String, String> commitUserData) {
    return commitUserData.get(COMMIT_ACTIVE_KEY);
  }

  /**
   * Gets the key id from the provided commit user data, for the given key reference number.
   */
  public static String getKeyIdFromCommit(String keyRef, Map<String, String> commitUserData) {
    String keyId = commitUserData.get(COMMIT_KEY_ID + keyRef);
    if (keyId == null) {
      throw new NoSuchElementException("No key id for key ref=" + keyRef);
    }
    return keyId;
  }

  /**
   * Gets the cookies (key-value pairs) for all the key ids, from the provided commit user data.
   *
   * @return the cookies for all key ids.
   */
  @SuppressWarnings("unchecked")
  public static KeyCookies getKeyCookiesFromCommit(Map<String, String> commitUserData) {
    Map<String, Map<String, String>> cookiesByKey = null;
    for (Map.Entry<String, String> dataEntry : commitUserData.entrySet()) {
      if (dataEntry.getKey().startsWith(COMMIT_KEY_ID)) {
        String keyId = dataEntry.getValue();
        String keyRef = dataEntry.getKey().substring(COMMIT_KEY_ID.length());
        String cookieString = commitUserData.get(COMMIT_KEY_COOKIE + keyRef);
        if (cookieString != null) {
          if (cookiesByKey == null) {
            cookiesByKey = new HashMap<>();
          }
          cookiesByKey.put(keyId, (Map<String, String>) Utils.fromJSONString(cookieString));
        }
      }
    }
    return cookiesByKey == null ? KeyCookies.EMPTY : new KeyCookies(cookiesByKey);
  }

  /**
   * Clear the oldest inactive key ids to keep only the most recent ones.
   * We don't clear all the inactive key ids just in the improbable case there would be pending
   * segment creations using previous key id(s) still in flight. This helps during the
   * heavy-load test where re-encryption has a crazy rate, and this is really safe in prod.
   */
  public static void clearOldInactiveKeyIdsFromCommit(Map<String, String> commitUserData) {
    // List the inactive key references.
    String activeKeyRef = getActiveKeyRefFromCommit(commitUserData);
    List<Integer> inactiveKeyRefs = new ArrayList<>();
    for (String dataKey : commitUserData.keySet()) {
      if (dataKey.startsWith(COMMIT_KEY_ID)) {
        String keyRef = dataKey.substring(COMMIT_KEY_ID.length());
        if (!keyRef.equals(activeKeyRef)) {
          inactiveKeyRefs.add(Integer.parseInt(keyRef));
        }
      }
    }
    // Clear them except the most recent ones.
    if (inactiveKeyRefs.size() > INACTIVE_KEY_IDS_TO_KEEP) {
      inactiveKeyRefs.sort(Comparator.naturalOrder());
      for (Integer keyRef : inactiveKeyRefs.subList(0, inactiveKeyRefs.size() - INACTIVE_KEY_IDS_TO_KEEP)) {
        commitUserData.remove(COMMIT_KEY_ID + keyRef);
        commitUserData.remove(COMMIT_KEY_COOKIE + keyRef);
        log.info("{} removing inactive key ref={}", ENCRYPTION_LOG_PREFIX, keyRef);
      }
    }
  }

  /**
   * Key cookie key-value pairs optionally associated to a key id in the commit user data.
   */
  public static class KeyCookies {

    private static final KeyCookies EMPTY = new KeyCookies(Map.of());

    private final Map<String, Map<String, String>> cookiesByKey;

    private KeyCookies(Map<String, Map<String, String>> cookiesByKey) {
      this.cookiesByKey = cookiesByKey;
    }

    /**
     * Gets the cookie corresponding to the provided key id; or null if none.
     */
    @Nullable
    public Map<String, String> get(String keyId) {
      return cookiesByKey.get(keyId);
    }
  }
}
