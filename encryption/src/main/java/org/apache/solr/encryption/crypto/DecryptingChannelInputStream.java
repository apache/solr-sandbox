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

import org.apache.solr.update.TransactionLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.solr.encryption.crypto.AesCtrUtil.AES_BLOCK_SIZE;
import static org.apache.solr.encryption.crypto.AesCtrUtil.IV_LENGTH;
import static org.apache.solr.encryption.crypto.DecryptingIndexInput.AES_BLOCK_SIZE_MOD_MASK;
import static org.apache.solr.encryption.crypto.DecryptingIndexInput.BUFFER_CAPACITY;

/**
 * {@link org.apache.solr.common.util.FastInputStream} that reads from a {@link FileChannel} and
 * decrypts data on the fly.
 * <p>The encryption transformation is AES/CTR/NoPadding. It decrypts the data previously encrypted
 * with an {@link EncryptingOutputStream}.
 * <p>It first reads the CTR Initialization Vector (IV). This random IV is not encrypted. Then it
 * can decrypt the rest of the file.
 *
 * @see EncryptingOutputStream
 * @see AesCtrEncrypter
 */
public class DecryptingChannelInputStream extends TransactionLog.ChannelFastInputStream {

  private final long offset;
  private final byte[] iv;
  private final AesCtrEncrypter encrypter;
  private final ByteBuffer inBuffer;
  private final byte[] outBuffer;
  private int outPos;
  private int outSize;
  private int padding;
  private long filePointer;

  /**
   * @param channel  The delegate {@link FileChannel} to read and decrypt data from.
   * @param offset   Base offset in the {@link FileChannel}. The IV at the beginning of
   *                 the file starts at this offset. {@link #seek(long)} positions are
   *                 relative to this base offset + {@link AesCtrUtil#IV_LENGTH}.
   * @param position Initial read position, relative to the base offset. Setting a positive
   *                 position is equivalent to setting a zero position and then calling
   *                 {@link #setPosition(long)}.
   * @param key      The encryption key secret. It is cloned internally, its content
   *                 is not modified, and no reference to it is kept.
   * @param factory  The factory to use to create one instance of {@link AesCtrEncrypter}.
   */
  public DecryptingChannelInputStream(FileChannel channel,
                                      long offset,
                                      long position,
                                      byte[] key,
                                      AesCtrEncrypterFactory factory)
    throws IOException {
    this(channel, offset, position, key, factory, BUFFER_CAPACITY);
  }

  /**
   * @param channel  The delegate {@link FileChannel} to read and decrypt data from.
   * @param offset   Base offset in the {@link FileChannel}. The IV at the beginning of
   *                 the file starts at this offset. {@link #seek(long)} positions are
   *                 relative to this base offset + {@link AesCtrUtil#IV_LENGTH}.
   * @param position Initial read position, relative to the base offset. Setting a positive
   *                 position is equivalent to setting a zero position and then calling
   *                 {@link #setPosition(long)}.
   * @param key      The encryption key secret. It is cloned internally, its content
   *                 is not modified, and no reference to it is kept.
   * @param factory  The factory to use to create one instance of {@link AesCtrEncrypter}.
   * @param bufferCapacity The encryption buffer capacity. It must be a multiple of {@link AesCtrUtil#AES_BLOCK_SIZE}.
   */
  public DecryptingChannelInputStream(FileChannel channel,
                                      long offset,
                                      long position,
                                      byte[] key,
                                      AesCtrEncrypterFactory factory,
                                      int bufferCapacity)
    throws IOException {
    super(channel, position);
    assert offset >= 0;
    assert position >= 0;
    this.offset = offset;
    iv = new byte[IV_LENGTH];
    channel.read(ByteBuffer.wrap(iv, 0, iv.length), offset);
    encrypter = factory.create(key, iv);
    assert bufferCapacity % AES_BLOCK_SIZE == 0;
    inBuffer = ByteBuffer.allocate(bufferCapacity);
    outBuffer = new byte[bufferCapacity + AES_BLOCK_SIZE];
    setPosition(position);
  }

  /**
   * Gets the IV read at the beginning of the input stream.
   */
  public byte[] getIv() {
    return iv;
  }

  @Override
  public int readWrappedStream(byte[] target, int offset, int length) throws IOException {
    assert offset >= 0 && length >= 0;
    assert offset + length <= target.length;
    int numDecrypted = 0;
    while (length > 0) {
      // Transfer decrypted bytes from outBuffer.
      int outRemaining = outSize - outPos;
      if (outRemaining > 0) {
        if (length <= outRemaining) {
          System.arraycopy(outBuffer, outPos, target, offset, length);
          outPos += length;
          numDecrypted += length;
          return numDecrypted;
        }
        System.arraycopy(outBuffer, outPos, target, offset, outRemaining);
        outPos += outRemaining;
        numDecrypted += outRemaining;
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
      int numRead;
      if (length < inRemaining) {
        inBuffer.limit(inBuffer.position() + length);
        numRead = ch.read(inBuffer, filePointer);
        inBuffer.limit(inBuffer.capacity());
      } else {
        numRead = ch.read(inBuffer, filePointer);
      }
      if (numRead == -1) {
        return false;
      }
      filePointer += numRead;
    }
    return true;
  }

  private void decryptBuffer() {
    int inPos = inBuffer.position();
    assert inPos > padding : "inPos=" + inPos + " padding=" + padding;
    encrypter.process(inBuffer.array(), 0, inPos, outBuffer, 0);
    outSize = inPos;
    inBuffer.position(0);
    outPos = padding;
    padding = 0;
  }

  @Override
  public void seek(long position) {
    assert position >= 0;
    if (position <= readFromStream && position >= getBufferPos()) {
      // Seek within the FastInputStream buffer.
      pos = (int) (position - getBufferPos());
    } else {
      long channelPosition = filePointer - offset - IV_LENGTH;
      long currentPosition = channelPosition - (outSize - outPos);
      if (position >= currentPosition && position <= channelPosition) {
        // The target position is within the buffered output. Just move the output buffer position.
        outPos += (int) (position - currentPosition);
        assert position == channelPosition - (outSize - outPos);
      } else {
        setPosition(position);
      }
      readFromStream = position;
      end = pos = 0;
    }
    assert position() == position;
  }

  private void setPosition(long position) {
    outPos = outSize = 0;
    // Compute the counter by ignoring the IV and the channel offset, if any.
    long counter = position / AES_BLOCK_SIZE;
    encrypter.init(counter);
    padding = (int) (position & AES_BLOCK_SIZE_MOD_MASK);
    inBuffer.position(padding);
    filePointer = position + offset + IV_LENGTH;
  }
}
