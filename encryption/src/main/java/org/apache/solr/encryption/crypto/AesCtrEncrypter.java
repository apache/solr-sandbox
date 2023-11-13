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
package org.apache.solr.encryption.crypto;

/**
 * Stateful Encrypter specialized for AES algorithm in CTR (counter) mode with no padding.
 * <p>In CTR mode, encryption and decryption are actually the same operation, so this API does not require specifying
 * whether it is used to encrypt or decrypt.
 * <p>An {@link AesCtrEncrypter} must be first {@link #init(long) initialized} before it can be used to
 * {@link #process encrypt/decrypt}.
 * <p>Not thread safe.
 */
public interface AesCtrEncrypter extends Cloneable {

  /**
   * Initializes this encrypter at the provided CTR block counter (counter of blocks of size
   * {@link AesCtrUtil#AES_BLOCK_SIZE}).
   * <p>For example, the data byte at index i is inside the block at counter = i / {@link AesCtrUtil#AES_BLOCK_SIZE}.
   * CTR mode computes an IV for this block based on the initial IV (at counter 0) and the provided counter. This allows
   * efficient random access to encrypted data. Only the target block needs to be decrypted.
   * <p>This method must be called first. Then the next call to {@link #process} will start at the beginning of the
   * block: the first byte of input data must be the first byte of the block.
   */
  void init(long counter);

  /**
   * Encrypts/decrypts the provided input buffer data and stores the encrypted/decrypted data in an output buffer.
   * In CTR mode, encryption and decryption are actually the same operation.
   * <p>Do not call this method when this {@link AesCtrEncrypter} is not {@link #init(long) initialized}.
   * <p>This method takes care of incrementing the CTR counter while encrypting/decrypting the data. It can be called
   * repeatedly without calling {@link #init(long)} again. {@link #init(long)} is called only to jump to a given block.
   *
   * @param inBuffer  Input data buffer.
   * @param inOffset  Offset (inclusive) of the first byte of input data.
   * @param length    Number of input bytes to process.
   * @param outBuffer Output data buffer where to put the processed bytes. Its capacity must be greater than or equal
   *                  {@code length}.
   * @param outOffset Output offset (inclusive) where to put the first output byte.
   */
  void process(byte[] inBuffer, int inOffset, int length, byte[] outBuffer, int outOffset);

  /**
   * Clones this {@link AesCtrEncrypter} for efficiency as it clones the internal encryption key and IV.
   * The returned clone must be initialized by calling {@link #init(long)} first.
   */
  AesCtrEncrypter clone();
}