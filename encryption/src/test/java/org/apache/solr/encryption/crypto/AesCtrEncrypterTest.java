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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.apache.solr.encryption.crypto.AesCtrUtil.*;

/**
 * Tests {@link AesCtrEncrypter} implementations.
 */
public class AesCtrEncrypterTest extends RandomizedTest {

  /**
   * Verifies that {@link AesCtrEncrypter} implementations encrypt and decrypt data exactly the
   * same way. They produce the same encrypted data and can decrypt each other.
   */
  @Test
  public void testEncryptionDecryption() {
    for (int i = 0; i < 100; i++) {
      ByteBuffer clearData = generateRandomData(randomIntBetween(5000, 10000));
      byte[] key = randomBytesOfLength(randomIntBetween(2, 4) * 8);
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
      return randomBoolean() ? CipherAesCtrEncrypter.FACTORY : LightAesCtrEncrypter.FACTORY;
    }
    return CipherAesCtrEncrypter.FACTORY;
  }

  private static ByteBuffer generateRandomData(int numBytes) {
    ByteBuffer buffer = ByteBuffer.allocate(numBytes);
    for (int i = 0; i < numBytes; i++) {
      buffer.put((byte) randomInt());
    }
    buffer.position(0);
    return buffer;
  }

  private ByteBuffer crypt(ByteBuffer inputBuffer, AesCtrEncrypter encrypter) {
    encrypter = randomClone(encrypter);
    encrypter.init(0);
    int inputInitialPosition = inputBuffer.position();
    ByteBuffer outputBuffer = ByteBuffer.allocate(inputBuffer.capacity());
    while (inputBuffer.remaining() > 0) {
      int length = Math.min(randomIntBetween(0, 50) + 1, inputBuffer.remaining());
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
    return randomBoolean() ? encrypter.clone() : encrypter;
  }
}