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

import java.io.IOException;
import java.util.function.Function;

/**
 * Manages encryption keys and defines which index files to encrypt.
 * Supplies the encryption key secrets corresponding to provided key ids.
 */
public interface KeyManager {

  /**
   * Indicates whether the provided file is encryptable based on its name.
   * <p/>
   * Segments files ({@link IndexFileNames#SEGMENTS} or {@link IndexFileNames#PENDING_SEGMENTS}) are never
   * passed as parameter because they are filtered before calling this method (they must not be encrypted).
   */
  boolean isEncryptable(String fileName);

  /**
   * Gets the cookie corresponding to a given key.
   * The cookie is an additional binary data to provide to get the key secret.
   *
   * @throws java.util.NoSuchElementException if the key is unknown.
   */
  byte[] getKeyCookie(String keyId) throws IOException;

  /**
   * Gets the encryption key secret corresponding to the provided key id.
   *
   * @param keyId          Key id which identifies uniquely the encryption key.
   * @param keyRef         Key internal reference number to provide to the cookie supplier to retrieve the
   *                       corresponding cookie, if any.
   * @param cookieSupplier Takes the key reference number as input and supplies an additional binary data
   *                       cookie required to get the key secret. This supplier may not be called if the
   *                       key secret is in the transient memory cache. It may return null if there are no
   *                       cookies.
   * @return The key secret bytes. It must be either 16, 24 or 32 bytes long. The caller is not permitted
   * to modify its content. Returns null if the key is known but has no secret bytes, in this case the data
   * is not encrypted.
   * @throws java.util.NoSuchElementException if the key is unknown.
   */
  byte[] getKeySecret(String keyId, String keyRef, Function<String, byte[]> cookieSupplier) throws IOException;

  /**
   * Supplies the {@link KeyManager}.
   */
  interface Supplier {

    /** This supplier may be configured with parameters defined in solrconfig.xml. */
    void init(NamedList<?> args);

    /** Gets the {@link KeyManager}. */
    KeyManager getKeyManager();
  }
}
