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

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.apache.solr.encryption.crypto.AesCtrUtil.*;

/**
 * {@link IndexOutput} that encrypts data and writes to a delegate {@link IndexOutput} on the fly.
 * <p>It encrypts with the AES algorithm in CTR (counter) mode with no padding. It is appropriate for random access of
 * the read-only index files. Use a {@link DecryptingIndexInput} to decrypt this file.
 * <p>It generates a cryptographically strong random CTR Initialization Vector (IV). This random IV is not encrypted and
 * is skipped by any {@link DecryptingIndexInput} reading the written data. Then it can encrypt the rest of the file
 * which probably contains a header and footer.
 * <p>It is a {@link FilterIndexOutput}, so it is possible to {@link FilterIndexOutput#unwrap} it.
 *
 * @see DecryptingIndexInput
 * @see AesCtrEncrypter
 */
public class EncryptingIndexOutput extends FilterIndexOutput {

  /**
   * Must be a multiple of {@link AesCtrUtil#AES_BLOCK_SIZE}.
   */
  public static final int BUFFER_CAPACITY = 64 * AES_BLOCK_SIZE; // 1024

  private final AesCtrEncrypter encrypter;
  private final byte[] inBuffer;
  private final byte[] outBuffer;
  private final byte[] oneByteBuf;
  private final Checksum clearChecksum;
  private int inSize;
  private long filePointer;
  private boolean closed;

  /**
   * @param indexOutput The delegate {@link IndexOutput} to write encrypted data to. Its current file pointer may be
   *                    greater than or equal to zero, this allows for example the caller to first write some special
   *                    encryption header followed by a key id, to identify the key secret used to encrypt.
   * @param key         The encryption key secret. It is cloned internally, its content is not modified, and no
   *                    reference to it is kept.
   * @param factory     The factory to use to create one instance of {@link AesCtrEncrypter}. This instance may be cloned.
   */
  public EncryptingIndexOutput(IndexOutput indexOutput, byte[] key, AesCtrEncrypterFactory factory)
    throws IOException {
    this(indexOutput, key, factory, BUFFER_CAPACITY);
  }

  /**
   * @param indexOutput    The delegate {@link IndexOutput} to write encrypted data to. Its current file pointer may be
   *                       greater than or equal to zero, this allows for example the caller to first write some special
   *                       encryption header followed by a key id, to identify the key secret used to encrypt.
   * @param key            The encryption key secret. It is cloned internally, its content is not modified, and no
   *                       reference to it is kept.
   * @param factory        The factory to use to create one instance of {@link AesCtrEncrypter}. This instance may be
   *                       cloned.
   * @param bufferCapacity The encryption buffer capacity. It must be a multiple of {@link AesCtrUtil#AES_BLOCK_SIZE}.
   */
  public EncryptingIndexOutput(IndexOutput indexOutput,
                               byte[] key,
                               AesCtrEncrypterFactory factory,
                               int bufferCapacity)
    throws IOException {
    super("Encrypting " + indexOutput, indexOutput.getName(), indexOutput);
    byte[] iv = generateRandomIv();
    encrypter = factory.create(key, iv);
    encrypter.init(0);
    // IV is written at the beginning of the index output. It's public.
    // Even if the delegate indexOutput is positioned after the initial IV, this index output file pointer is 0 initially.
    indexOutput.writeBytes(iv, 0, iv.length);
    assert bufferCapacity % AES_BLOCK_SIZE == 0;
    inBuffer = new byte[bufferCapacity];
    outBuffer = new byte[bufferCapacity];
    oneByteBuf = new byte[1];
    // Compute the checksum to skip the initial IV, because an external checksum checker will not see it.
    clearChecksum = new BufferedChecksum(new CRC32());
  }

  /**
   * Generates a cryptographically strong CTR random IV of length {@link AesCtrUtil#IV_LENGTH}.
   */
  protected byte[] generateRandomIv() {
    return generateRandomAesCtrIv(SecureRandomProvider.get());
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        if (inSize != 0) {
          encryptBufferAndWrite();
        }
      } finally {
        out.close();
      }
    }
  }

  @Override
  public long getFilePointer() {
    // With AES/CTR/NoPadding, the encrypted and decrypted data have the same length.
    // We return here the file pointer excluding the initial IV length at the beginning of the file.
    return filePointer;
  }

  @Override
  public long getChecksum() {
    // The checksum is computed on the clear data, excluding the initial IV.
    return clearChecksum.getValue();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    oneByteBuf[0] = b;
    writeBytes(oneByteBuf, 0, oneByteBuf.length);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > b.length) {
      throw new IllegalArgumentException("Invalid write buffer parameters (offset=" + offset + ", length=" + length + ", arrayLength=" + b.length + ")");
    }
    clearChecksum.update(b, offset, length);
    filePointer += length;
    while (length > 0) {
      int remaining = inBuffer.length - inSize;
      if (length < remaining) {
        System.arraycopy(b, offset, inBuffer, inSize, length);
        inSize += length;
        break;
      } else {
        System.arraycopy(b, offset, inBuffer, inSize, remaining);
        inSize += remaining;
        offset += remaining;
        length -= remaining;
        encryptBufferAndWrite();
      }
    }
  }

  private void encryptBufferAndWrite() throws IOException {
    assert inSize > 0;
    encrypter.process(inBuffer, 0, inSize, outBuffer, 0);
    out.writeBytes(outBuffer, 0, inSize);
    inSize = 0;
  }
}