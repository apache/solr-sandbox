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

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides encryption key secrets corresponding to provided key ids and defines which index files to encrypt.
 */
public interface KeySupplier extends Closeable {

  /**
   * Indicates whether the provided file should be encrypted based on its name.
   * <p/>
   * Segments files ({@link IndexFileNames#SEGMENTS} or {@link IndexFileNames#PENDING_SEGMENTS}) are never
   * passed as parameter because they are filtered before calling this method (they must not be encrypted).
   */
  boolean shouldEncrypt(String fileName);

  /**
   * Gets the optional cookie corresponding to a given key.
   * The cookie is a set of key-value pairs to provide to {@link #getKeySecret}.
   *
   * @param params optional parameters in addition to the key id; or null if none.
   * @return the key-value pairs; or null if none.
   * @throws java.util.NoSuchElementException if the key is unknown.
   */
  @Nullable
  Map<String, String> getKeyCookie(String keyId, Map<String, String> params) throws IOException;

  /**
   * Gets the encryption key secret corresponding to the provided key id.
   * Typically, this {@link KeySupplier} holds a cache of key secrets, and may load the key secret if it is
   * not in the cache or after expiration. In this case, this method may need to get additional data from
   * the key cookie to load the key secret.
   *
   * @param keyId          Key id which identifies uniquely the encryption key.
   * @param cookieSupplier Takes the key id as input and supplies the optional key cookie key-value pairs
   *                       that may be needed to retrieve the key secret. It may return null if there are
   *                       no cookies for the key.
   * @return The key secret bytes. It must be either 16, 24 or 32 bytes long. The caller is not permitted
   * to modify its content. Returns null if the key is known but has no secret bytes, in this case the data
   * is not encrypted.
   * @throws java.util.NoSuchElementException if the key is unknown.
   */
  byte[] getKeySecret(String keyId, Function<String, Map<String, String>> cookieSupplier) throws IOException;

  /**
   * Creates {@link KeySupplier}.
   */
  interface Factory {

    /**
     * Initializes this factory.
     *
     * @param args non-null list of initialization parameters (may be empty).
     */
    void init(NamedList<?> args, CoreContainer coreContainer);

    /** Creates a {@link KeySupplier}. */
    KeySupplier create() throws IOException;
  }
}
