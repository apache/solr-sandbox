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

import java.nio.ByteBuffer;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

import static org.apache.solr.encryption.crypto.AesCtrUtil.*;

/**
 * Tests {@link AesCtrEncrypter} implementations.
 */
public class AesCtrEncrypterTest extends LuceneTestCase {

  /**
   * Verifies that {@link AesCtrEncrypter} implementations encrypt and decrypt data exactly the
   * same way. They produce the same encrypted data and can decrypt each other.
   */
  @Test
  public void testEncryptionDecryption() {
    for (int i = 0; i < 100; i++) {
      ByteBuffer clearData = generateRandomData(10000);
      byte[] key = generateRandomBytes(AES_BLOCK_SIZE);
      byte[] iv = generateRandomAesCtrIv(SecureRandomProvider.get());
      AesCtrEncrypter encrypter1 = encrypterFactory().create(key, iv);
      AesCtrEncrypter encrypter2 = encrypterFactory().create(key, iv);

      ByteBuffer encryptedDataLight = crypt(clearData, encrypter1);
      ByteBuffer encryptedDataCipher = crypt(clearData, encrypter2);
      assertEquals(encryptedDataCipher, encryptedDataLight);

      ByteBuffer decryptedData = crypt(encryptedDataLight, encrypter1);
      assertEquals(clearData, decryptedData);
      decryptedData = crypt(encryptedDataLight, encrypter2);
      assertEquals(clearData, decryptedData);
    }
  }

  private AesCtrEncrypterFactory encrypterFactory() {
    if (LightAesCtrEncrypter.isSupported()) {
      return random().nextBoolean() ? CipherAesCtrEncrypter.FACTORY : LightAesCtrEncrypter.FACTORY;
    }
    return CipherAesCtrEncrypter.FACTORY;
  }

  private static ByteBuffer generateRandomData(int numBytes) {
    ByteBuffer buffer = ByteBuffer.allocate(numBytes);
    for (int i = 0; i < numBytes; i++) {
      buffer.put((byte) random().nextInt());
    }
    buffer.position(0);
    return buffer;
  }

  private static byte[] generateRandomBytes(int numBytes) {
    byte[] b = new byte[numBytes];
    // Random.nextBytes(byte[]) does not produce good enough randomness here,
    // it has a bias to produce 0 and -1 bytes.
    for (int i = 0; i < numBytes; i++) {
      b[i] = (byte) random().nextInt();
    }
    return b;
  }

  private ByteBuffer crypt(ByteBuffer inputBuffer, AesCtrEncrypter encrypter) {
    encrypter = randomClone(encrypter);
    encrypter.init(0);
    int inputInitialPosition = inputBuffer.position();
    ByteBuffer outputBuffer = ByteBuffer.allocate(inputBuffer.capacity());
    while (inputBuffer.remaining() > 0) {
      int length = Math.min(random().nextInt(51) + 1, inputBuffer.remaining());
      ByteBuffer inputSlice = inputBuffer.slice();
      inputSlice.limit(inputSlice.position() + length);
      encrypter.process(inputSlice, outputBuffer);
      inputBuffer.position(inputBuffer.position() + length);
    }
    inputBuffer.position(inputInitialPosition);
    outputBuffer.position(0);
    return outputBuffer;
  }

  private static AesCtrEncrypter randomClone(AesCtrEncrypter encrypter) {
    return random().nextBoolean() ? encrypter.clone() : encrypter;
  }
}