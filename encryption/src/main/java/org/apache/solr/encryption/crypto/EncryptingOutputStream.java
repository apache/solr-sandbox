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

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.solr.encryption.crypto.AesCtrUtil.AES_BLOCK_SIZE;
import static org.apache.solr.encryption.crypto.AesCtrUtil.generateRandomAesCtrIv;
import static org.apache.solr.encryption.crypto.EncryptingIndexOutput.BUFFER_CAPACITY;

/**
 * {@link OutputStream} that encrypts data and writes to a delegate {@link OutputStream} on the fly.
 * <p>The encryption transformation is AES/CTR/NoPadding. Use a {@link DecryptingInputStream} to
 * decrypt the encrypted data.
 * <p>It generates a cryptographically strong random CTR Initialization Vector (IV). This random IV
 * is not encrypted and is skipped by any {@link DecryptingInputStream} reading the written data.
 * Then it can encrypt the rest of the file.
 *
 * @see DecryptingInputStream
 * @see AesCtrEncrypter
 */
public class EncryptingOutputStream extends OutputStream {

  private final OutputStream outputStream;
  private final byte[] iv;
  private final AesCtrEncrypter encrypter;
  private final byte[] inBuffer;
  private final byte[] outBuffer;
  private final byte[] oneByteBuf;
  private int inSize;
  private int padding;
  private boolean closed;

  /**
   * @param outputStream The delegate {@link OutputStream} to write encrypted data to.
   * @param key          The encryption key secret. It is cloned internally, its content
   *                     is not modified, and no reference to it is kept.
   * @param factory      The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public EncryptingOutputStream(OutputStream outputStream, byte[] key, AesCtrEncrypterFactory factory)
    throws IOException {
    this(outputStream, 0L, null, key, factory);
  }

  /**
   * @param outputStream The delegate {@link OutputStream} to write encrypted data to.
   * @param position     The position in the output stream. A non-zero position means the
   *                     output is reopened to append more data, in this case the iv should
   *                     be provided and not null.
   * @param iv           The IV to use (not written) if the position is greater than zero;
   *                     or null to generate a random one and write it at the beginning of
   *                     the output.
   * @param key          The encryption key secret. It is cloned internally, its content
   *                     is not modified, and no reference to it is kept.
   * @param factory      The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public EncryptingOutputStream(OutputStream outputStream,
                                long position,
                                byte[] iv,
                                byte[] key,
                                AesCtrEncrypterFactory factory)
    throws IOException {
    this(outputStream, position, iv, key, factory, BUFFER_CAPACITY);
  }

  /**
   * @param outputStream The delegate {@link OutputStream} to write encrypted data to.
   * @param position     The position in the output stream. A non-zero position means the
   *                     output is reopened to append more data, in this case the iv should
   *                     be provided and not null.
   * @param iv           The IV to use (not written) if the position is greater than zero;
   *                     or null to generate a random one and write it at the beginning of
   *                     the output.
   * @param key          The encryption key secret. It is cloned internally, its content
   *                     is not modified, and no reference to it is kept.
   * @param factory      The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public EncryptingOutputStream(OutputStream outputStream,
                                long position,
                                byte[] iv,
                                byte[] key,
                                AesCtrEncrypterFactory factory,
                                int bufferCapacity)
    throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("Invalid position " + position);
    }
    this.outputStream = outputStream;
    assert bufferCapacity % AES_BLOCK_SIZE == 0;
    inBuffer = new byte[bufferCapacity];
    outBuffer = new byte[bufferCapacity + AES_BLOCK_SIZE];
    oneByteBuf = new byte[1];
    long counter;
    if (position == 0) {
      iv = generateRandomIv();
      // Write the IV at the beginning of the output stream. It's public.
      outputStream.write(iv, 0, iv.length);
      counter = 0;
    } else if (iv == null) {
      throw new IllegalArgumentException("IV must be provided when position is not zero");
    } else {
      counter = position / AES_BLOCK_SIZE;
      padding = (int) (position & (AES_BLOCK_SIZE - 1));
      inSize = padding;
    }
    this.iv = iv;
    encrypter = factory.create(key, iv);
    encrypter.init(counter);
  }

  /**
   * Generates a cryptographically strong CTR random IV of length {@link AesCtrUtil#IV_LENGTH}.
   */
  protected byte[] generateRandomIv() {
    return generateRandomAesCtrIv(SecureRandomProvider.get());
  }

  /**
   * Gets the IV written at the beginning of the output stream.
   */
  public byte[] getIv() {
    return iv;
  }

  @Override
  public void flush() throws IOException {
    if (inSize > padding) {
      encryptBufferAndWrite();
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        flush();
      } finally {
        outputStream.close();
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    oneByteBuf[0] = (byte) b;
    write(oneByteBuf, 0, oneByteBuf.length);
  }

  @Override
  public void write(byte[] b, int offset, int length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > b.length) {
      throw new IllegalArgumentException("Invalid write buffer parameters (offset=" + offset
                                           + ", length=" + length
                                           + ", arrayLength=" + b.length + ")");
    }
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
    assert inSize > padding : "inSize=" + inSize + " padding=" + padding;
    encrypter.process(inBuffer, 0, inSize, outBuffer, 0);
    outputStream.write(outBuffer, padding, inSize - padding);
    inSize = 0;
    padding = 0;
  }
}