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
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.solr.encryption.crypto.AesCtrUtil.AES_BLOCK_SIZE;
import static org.apache.solr.encryption.crypto.AesCtrUtil.IV_LENGTH;

/**
 * {@link InputStream} that reads from a delegate {@link InputStream} and decrypts data on the fly.
 * <p>The encryption transformation is AES/CTR/NoPadding. It decrypts the data previously encrypted
 * with an {@link EncryptingOutputStream}.
 * <p>It first reads the CTR Initialization Vector (IV). This random IV is not encrypted. Then it
 * can decrypt the rest of the file.
 *
 * @see EncryptingOutputStream
 * @see AesCtrEncrypter
 */
public class DecryptingInputStream extends InputStream {

  /**
   * Must be a multiple of {@link AesCtrUtil#AES_BLOCK_SIZE}.
   * Benchmarks showed that 6 x {@link AesCtrUtil#AES_BLOCK_SIZE} is a good buffer size.
   */
  private static final int BUFFER_CAPACITY = 6 * AES_BLOCK_SIZE; // 96 B

  private final InputStream inputStream;
  private final AesCtrEncrypter encrypter;
  private final ByteBuffer inBuffer;
  private final ByteBuffer outBuffer;
  private final byte[] inArray;
  private final byte[] oneByteBuf;
  private boolean closed;

  /**
   * @param inputStream The delegate {@link InputStream} to read and decrypt data from.
   * @param key         The encryption key secret. It is cloned internally, its content
   *                    is not modified, and no reference to it is kept.
   * @param factory     The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public DecryptingInputStream(InputStream inputStream, byte[] key, AesCtrEncrypterFactory factory) throws IOException {
    this.inputStream = inputStream;
    this.encrypter = createEncrypter(inputStream, key, factory);
    encrypter.init(0);
    inBuffer = ByteBuffer.allocate(getBufferCapacity());
    outBuffer = ByteBuffer.allocate(getBufferCapacity() + AES_BLOCK_SIZE);
    outBuffer.limit(0);
    assert inBuffer.hasArray() && outBuffer.hasArray();
    assert inBuffer.arrayOffset() == 0;
    inArray = inBuffer.array();
    oneByteBuf = new byte[1];
  }

  /**
   * Creates the {@link AesCtrEncrypter} based on the secret key and the IV at the beginning
   * of the input stream.
   */
  private static AesCtrEncrypter createEncrypter(InputStream inputStream,
                                                 byte[] key,
                                                 AesCtrEncrypterFactory factory)
    throws IOException {
    byte[] iv = new byte[IV_LENGTH];
    int n = inputStream.read(iv, 0, iv.length);
    if (n != iv.length) {
      throw new IOException("Missing IV");
    }
    return factory.create(key, iv);
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
      inputStream.close();
    }
  }

  @Override
  public int read() throws IOException {
    int n = read(oneByteBuf, 0, 1);
    return n == -1 ? -1 : oneByteBuf[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > b.length) {
      throw new IllegalArgumentException(
        "Invalid read buffer parameters (offset=" + offset + ", length=" + length
          + ", arrayLength=" + b.length + ")");
    }
    int numRead = 0;
    while (length > 0) {
      // Transfer decrypted bytes from outBuffer.
      int outRemaining = outBuffer.remaining();
      if (outRemaining > 0) {
        if (length <= outRemaining) {
          outBuffer.get(b, offset, length);
          numRead += length;
          return numRead;
        }
        outBuffer.get(b, offset, outRemaining);
        numRead += outRemaining;
        assert outBuffer.remaining() == 0;
        offset += outRemaining;
        length -= outRemaining;
      }

      if (!readToFillBuffer(length)) {
        return numRead == 0 ? -1 : numRead;
      }
      decryptBuffer();
    }
    return numRead;
  }

  private boolean readToFillBuffer(int length) throws IOException {
    assert length > 0;
    int inRemaining = inBuffer.remaining();
    if (inRemaining > 0) {
      int position = inBuffer.position();
      int numBytesToRead = Math.min(inRemaining, length);
      int n = inputStream.read(inArray, position, numBytesToRead);
      if (n == -1) {
        return false;
      }
      inBuffer.position(position + n);
    }
    return true;
  }

  private void decryptBuffer() {
    inBuffer.flip();
    outBuffer.clear();
    encrypter.process(inBuffer, outBuffer);
    inBuffer.clear();
    outBuffer.flip();
  }
}