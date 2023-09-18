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
import java.nio.ByteBuffer;

import static org.apache.solr.encryption.crypto.AesCtrUtil.AES_BLOCK_SIZE;
import static org.apache.solr.encryption.crypto.AesCtrUtil.generateRandomAesCtrIv;

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

  /**
   * Must be a multiple of {@link AesCtrUtil#AES_BLOCK_SIZE}.
   */
  private static final int BUFFER_CAPACITY = 64 * AES_BLOCK_SIZE; // 1024

  private final OutputStream outputStream;
  private final AesCtrEncrypter encrypter;
  private final ByteBuffer inBuffer;
  private final ByteBuffer outBuffer;
  private final byte[] outArray;
  private final byte[] oneByteBuf;
  private boolean closed;

  /**
   * @param outputStream The delegate {@link OutputStream} to write encrypted data to.
   * @param key          The encryption key secret. It is cloned internally, its content
   *                     is not modified, and no reference to it is kept.
   * @param factory      The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public EncryptingOutputStream(OutputStream outputStream, byte[] key, AesCtrEncrypterFactory factory)
    throws IOException {
    this.outputStream = outputStream;

    byte[] iv = generateRandomIv();
    encrypter = factory.create(key, iv);
    encrypter.init(0);
    // IV is written at the beginning of the output stream. It's public.
    outputStream.write(iv, 0, iv.length);

    inBuffer = ByteBuffer.allocate(getBufferCapacity());
    outBuffer = ByteBuffer.allocate(getBufferCapacity() + AES_BLOCK_SIZE);
    assert inBuffer.hasArray() && outBuffer.hasArray();
    assert outBuffer.arrayOffset() == 0;
    outArray = outBuffer.array();
    oneByteBuf = new byte[1];
  }

  /**
   * Generates a cryptographically strong CTR random IV of length {@link AesCtrUtil#IV_LENGTH}.
   */
  protected byte[] generateRandomIv() {
    return generateRandomAesCtrIv(SecureRandomProvider.get());
  }

  /**
   * Gets the buffer capacity. It must be a multiple of {@link AesCtrUtil#AES_BLOCK_SIZE}.
   */
  protected int getBufferCapacity() {
    return BUFFER_CAPACITY;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        if (inBuffer.position() != 0) {
          encryptBufferAndWrite();
        }
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
      throw new IllegalArgumentException("Invalid write buffer parameters (offset=" + offset + ", length=" + length + ", arrayLength=" + b.length + ")");
    }
    while (length > 0) {
      int remaining = inBuffer.remaining();
      if (length < remaining) {
        inBuffer.put(b, offset, length);
        break;
      } else {
        inBuffer.put(b, offset, remaining);
        offset += remaining;
        length -= remaining;
        encryptBufferAndWrite();
      }
    }
  }

  private void encryptBufferAndWrite() throws IOException {
    assert inBuffer.position() != 0;
    inBuffer.flip();
    outBuffer.clear();
    encrypter.process(inBuffer, outBuffer);
    inBuffer.clear();
    outBuffer.flip();
    outputStream.write(outArray, 0, outBuffer.limit());
  }
}