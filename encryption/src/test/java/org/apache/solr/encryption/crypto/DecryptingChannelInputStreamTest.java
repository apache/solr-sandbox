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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.apache.solr.encryption.crypto.AesCtrUtil.IV_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link DecryptingChannelInputStream}.
 */
public class DecryptingChannelInputStreamTest extends RandomizedTest {

  @Test
  public void testReadAtPosition() throws Exception {
    byte[] source = randomBytesOfLength(2, 111);
    byte[] key = randomBytesOfLength(randomIntBetween(2, 4) * 8);
    byte[] keyId = randomBytesOfLength(3);

    // Write to an encrypting output stream.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(keyId, 0, keyId.length);
    EncryptingOutputStream eos = new EncryptingOutputStream(baos, key, encrypterFactory());
    write(source, 0, source.length, eos);
    eos.close();
    assertEquals(source.length + keyId.length + IV_LENGTH, baos.size());
    byte[] encryptedBytes = baos.toByteArray();
    Path testFile = newTempFile();
    FileChannel channel = FileChannel.open(testFile, StandardOpenOption.WRITE);
    int n = channel.write(ByteBuffer.wrap(encryptedBytes));
    assertEquals(encryptedBytes.length, n);
    channel.close();

    // Read a first part with a first decrypting channel.
    int firstPartLength = randomIntBetween(1, source.length - 1);
    channel = FileChannel.open(testFile, StandardOpenOption.READ);
    DecryptingChannelInputStream dc = new DecryptingChannelInputStream(channel, keyId.length, 0, key, encrypterFactory());
    byte[] decryptedBytes = new byte[source.length];
    read(dc, decryptedBytes, 0, firstPartLength);
    dc.close();
    assertTrue(Arrays.equals(source, 0, firstPartLength, decryptedBytes, 0, firstPartLength));

    // Seek and read the second part with a second decrypting channel.
    channel = FileChannel.open(testFile, StandardOpenOption.READ);
    dc = new DecryptingChannelInputStream(channel, keyId.length, 0, key, encrypterFactory());
    assertArrayEquals(eos.getIv(), dc.getIv());
    dc.seek(firstPartLength);
    assertEquals(firstPartLength, dc.position());
    read(dc, decryptedBytes, firstPartLength, source.length - firstPartLength);
    assertEquals(-1, dc.read(decryptedBytes, 0, 1));
    dc.close();
    assertArrayEquals(source, decryptedBytes);
  }

  private static AesCtrEncrypterFactory encrypterFactory() {
    return randomBoolean() ? CipherAesCtrEncrypter.FACTORY : LightAesCtrEncrypter.FACTORY;
  }

  private static void write(byte[] source, int offset, int length, OutputStream os) throws IOException {
    if (length > 0) {
      byte[] buffer = new byte[randomIntBetween(2, Math.max(length, 2))];
      int index = offset;
      int endIndex = offset + length;
      while (index < endIndex) {
        int numBytesToWrite = Math.min(randomIntBetween(1, buffer.length - 1), endIndex - index);
        System.arraycopy(source, index, buffer, 1, numBytesToWrite);
        index += numBytesToWrite;
        os.write(buffer, 1, numBytesToWrite);
      }
      assertEquals(endIndex, index);
    }
  }

  private static void read(InputStream is, byte[] output, int offset, int length) throws IOException {
    byte[] buffer = new byte[randomIntBetween(2, Math.max(length, 2))];
    int index = offset;
    int endIndex = offset + length;
    while (index < endIndex) {
      int numBytesToRead = Math.min(randomIntBetween(1, buffer.length - 1), endIndex - index);
      int n = is.read(output, index, numBytesToRead);
      assertEquals(numBytesToRead, n);
      index += numBytesToRead;
    }
    assertEquals(endIndex, index);
  }
}
