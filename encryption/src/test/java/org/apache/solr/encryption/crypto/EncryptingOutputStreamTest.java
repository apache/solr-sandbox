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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.solr.encryption.crypto.AesCtrUtil.IV_LENGTH;
import static org.apache.solr.encryption.crypto.CryptoTestUtil.encrypterFactory;
import static org.junit.Assert.*;

/**
 * Tests both {@link EncryptingOutputStream} and {@link DecryptingInputStream}.
 */
public class EncryptingOutputStreamTest extends RandomizedTest {

  @Test
  public void testWriteRead() throws Exception {
    byte[] source = randomBytesOfLength(2, 111);
    byte[] key = randomBytesOfLength(randomIntBetween(2, 4) * 8);

    // Write to an encrypting output stream.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EncryptingOutputStream eos = new EncryptingOutputStream(baos, key, encrypterFactory());
    write(source, 0, source.length, eos);
    eos.close();
    assertEquals(source.length + IV_LENGTH, baos.size());

    // Read with a decrypting input stream.
    InputStream is = new ByteArrayInputStream(baos.toByteArray());
    DecryptingInputStream dis = new DecryptingInputStream(is, key, encrypterFactory());
    byte[] decryptedBytes = new byte[source.length];
    read(dis, decryptedBytes, 0, source.length);
    assertEquals(-1, dis.read(decryptedBytes, 0, 1));
    dis.close();
    assertArrayEquals(source, decryptedBytes);
  }

  @Test
  public void testWriteAtPosition() throws Exception {
    byte[] source = randomBytesOfLength(2, 111);
    byte[] key = randomBytesOfLength(randomIntBetween(2, 4) * 8);

    // Write a first part to a first encrypting output stream.
    int firstPartLength = randomIntBetween(1, source.length - 1);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EncryptingOutputStream eos = new EncryptingOutputStream(baos, key, encrypterFactory());
    write(source, 0, firstPartLength, eos);
    eos.flush();
    assertEquals(firstPartLength + IV_LENGTH, baos.size());
    eos.close();
    assertEquals(firstPartLength + IV_LENGTH, baos.size());

    // Write the second part to a second encrypting output stream.
    byte[] iv = eos.getIv();
    eos = new EncryptingOutputStream(baos, firstPartLength, iv, key, encrypterFactory());
    write(source, firstPartLength, source.length - firstPartLength, eos);
    eos.close();
    eos.close();
    assertEquals(source.length + IV_LENGTH, baos.size());

    // Read with a decrypting input stream.
    InputStream is = new ByteArrayInputStream(baos.toByteArray());
    DecryptingInputStream dis = new DecryptingInputStream(is, key, encrypterFactory());
    byte[] decryptedBytes = new byte[source.length];
    read(dis, decryptedBytes, 0, source.length);
    assertEquals(-1, dis.read(decryptedBytes, 0, 1));
    dis.close();
    assertArrayEquals(source, decryptedBytes);
  }

  @Test
  public void testReadAtPosition() throws Exception {
    byte[] source = randomBytesOfLength(2, 111);
    byte[] key = randomBytesOfLength(randomIntBetween(2, 4) * 8);

    // Write to an encrypting output stream.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EncryptingOutputStream eos = new EncryptingOutputStream(baos, key, encrypterFactory());
    write(source, 0, source.length, eos);
    eos.close();
    assertEquals(source.length + IV_LENGTH, baos.size());

    // Read a first part with a first decrypting input stream.
    int firstPartLength = randomIntBetween(1, source.length - 1);
    InputStream is = new ByteArrayInputStream(baos.toByteArray());
    DecryptingInputStream dis = new DecryptingInputStream(is, key, encrypterFactory());
    byte[] decryptedBytes = new byte[source.length];
    read(dis, decryptedBytes, 0, firstPartLength);
    dis.close();

    // Read a second part with a second decrypting input stream.
    byte[] iv = eos.getIv();
    dis = new DecryptingInputStream(is, firstPartLength, iv, key, encrypterFactory());
    read(dis, decryptedBytes, firstPartLength, source.length - firstPartLength);
    assertEquals(-1, dis.read(decryptedBytes, 0, 1));
    dis.close();
    assertArrayEquals(source, decryptedBytes);
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
