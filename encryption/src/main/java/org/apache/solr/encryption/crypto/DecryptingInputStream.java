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
import static org.apache.solr.encryption.crypto.DecryptingIndexInput.AES_BLOCK_SIZE_MOD_MASK;
import static org.apache.solr.encryption.crypto.DecryptingIndexInput.BUFFER_CAPACITY;

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

  private final InputStream inputStream;
  private final byte[] iv;
  private final AesCtrEncrypter encrypter;
  private final ByteBuffer inBuffer;
  private final ByteBuffer outBuffer;
  private final byte[] inArray;
  private final byte[] oneByteBuf;
  private int padding;
  private boolean closed;

  /**
   * @param inputStream The delegate {@link InputStream} to read and decrypt data from.
   * @param key         The encryption key secret. It is cloned internally, its content
   *                    is not modified, and no reference to it is kept.
   * @param factory     The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public DecryptingInputStream(InputStream inputStream, byte[] key, AesCtrEncrypterFactory factory)
    throws IOException {
    this(inputStream, 0L, null, key, factory);
  }

  /**
   * @param inputStream The delegate {@link InputStream} to read and decrypt data from.
   * @param position    The position in the input stream. A non-zero position means the
   *                    input skips the beginning of the file, in this case the iv should
   *                    be provided and not null.
   * @param iv          The IV to use (not read) if the position is greater than zero;
   *                    or null to read it at the beginning of the input.
   * @param key         The encryption key secret. It is cloned internally, its content
   *                    is not modified, and no reference to it is kept.
   * @param factory     The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public DecryptingInputStream(InputStream inputStream,
                               long position,
                               byte[] iv,
                               byte[] key,
                               AesCtrEncrypterFactory factory)
    throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("Invalid position " + position);
    }
    this.inputStream = inputStream;
    inBuffer = ByteBuffer.allocate(BUFFER_CAPACITY);
    outBuffer = ByteBuffer.allocate(BUFFER_CAPACITY + AES_BLOCK_SIZE);
    outBuffer.limit(0);
    assert inBuffer.hasArray() && outBuffer.hasArray();
    assert inBuffer.arrayOffset() == 0;
    inArray = inBuffer.array();
    oneByteBuf = new byte[1];
    long counter;
    if (position == 0) {
      // Read the IV at the beginning of the input stream.
      iv = new byte[IV_LENGTH];
      int n = inputStream.read(iv, 0, iv.length);
      if (n != iv.length) {
        throw new IOException("Missing IV");
      }
      counter = 0;
    } else if (iv == null) {
      throw new IllegalArgumentException("IV must be provided when position is not zero");
    } else {
      counter = position / AES_BLOCK_SIZE;
      padding = (int) (position & AES_BLOCK_SIZE_MOD_MASK);
      inBuffer.position(padding);
    }
    this.iv = iv;
    encrypter = factory.create(key, iv);
    encrypter.init(counter);
  }

  /**
   * Gets the IV read at the beginning of the input stream.
   */
  public byte[] getIv() {
    return iv;
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
    int numDecrypted = 0;
    while (length > 0) {
      // Transfer decrypted bytes from outBuffer.
      int outRemaining = outBuffer.remaining();
      if (outRemaining > 0) {
        if (length <= outRemaining) {
          outBuffer.get(b, offset, length);
          numDecrypted += length;
          return numDecrypted;
        }
        outBuffer.get(b, offset, outRemaining);
        numDecrypted += outRemaining;
        assert outBuffer.remaining() == 0;
        offset += outRemaining;
        length -= outRemaining;
      }

      if (!readToFillBuffer(length)) {
        return numDecrypted == 0 ? -1 : numDecrypted;
      }
      decryptBuffer();
    }
    return numDecrypted;
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
    assert inBuffer.position() > padding : "position=" + inBuffer.position() + ", padding=" + padding;
    inBuffer.flip();
    outBuffer.clear();
    encrypter.process(inBuffer, outBuffer);
    inBuffer.clear();
    outBuffer.flip();
    if (padding > 0) {
      outBuffer.position(padding);
      padding = 0;
    }
  }
}