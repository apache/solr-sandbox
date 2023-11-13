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

import static org.apache.solr.encryption.crypto.AesCtrUtil.*;
import static org.junit.Assert.assertArrayEquals;

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
    for (int i = 0; i < 3000; i++) {
      try {
        byte[] clearData = generateRandomBytes(randomIntBetween(5000, 10000));
        byte[] key = randomBytesOfLength(randomIntBetween(2, 4) * 8);
        byte[] iv = generateRandomAesCtrIv(SecureRandomProvider.get());
        AesCtrEncrypter encrypter1 = encrypterFactory().create(key, iv);
        AesCtrEncrypter encrypter2 = encrypterFactory().create(key, iv);

        byte[] encryptedDataLight = crypt(clearData, encrypter1);
        byte[] encryptedDataCipher = crypt(clearData, encrypter2);
        assertArrayEquals(encryptedDataCipher, encryptedDataLight);

        byte[] decryptedData = crypt(encryptedDataLight, encrypter1);
        assertArrayEquals(clearData, decryptedData);
        decryptedData = crypt(encryptedDataLight, encrypter2);
        assertArrayEquals(clearData, decryptedData);
      } catch (RuntimeException e) {
        throw new RuntimeException("Exception at i=" + i, e);
      }
    }
  }

  private AesCtrEncrypterFactory encrypterFactory() {
    if (LightAesCtrEncrypter.isSupported()) {
      return randomBoolean() ? CipherAesCtrEncrypter.FACTORY : LightAesCtrEncrypter.FACTORY;
    }
    return CipherAesCtrEncrypter.FACTORY;
  }

  private static byte[] generateRandomBytes(int numBytes) {
    byte[] b = new byte[numBytes];
    // Random.nextBytes(byte[]) does not produce good enough randomness here,
    // it has a bias to produce 0 and -1 bytes.
    for (int i = 0; i < numBytes; i++) {
      b[i] = (byte) randomInt();
    }
    return b;
  }

  private byte[] crypt(byte[] inputBuffer, AesCtrEncrypter encrypter) {
    encrypter = randomClone(encrypter);
    encrypter.init(0);
    byte[] outputBuffer = new byte[inputBuffer.length];
    int inIndex = 0;
    int outIndex = 0;
    while (inIndex < inputBuffer.length) {
      int length = Math.min(randomIntBetween(0, 50) + 1, inputBuffer.length - inIndex);
      encrypter.process(inputBuffer, inIndex, length, outputBuffer, outIndex);
      inIndex += length;
      outIndex += length;
    }
    return outputBuffer;
  }

  private static AesCtrEncrypter randomClone(AesCtrEncrypter encrypter) {
    return randomBoolean() ? encrypter.clone() : encrypter;
  }
}