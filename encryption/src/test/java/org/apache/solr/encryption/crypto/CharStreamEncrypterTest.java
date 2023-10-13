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

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests {@link CharStreamEncrypter}. */
public class CharStreamEncrypterTest extends RandomizedTest {

  @Test
  public void testEmptyString() throws Exception {
    checkEncryptionDecryption("", new CharStreamEncrypter(encrypterFactory()));
  }

  @Test
  public void testRandomString() throws Exception {
    CharStreamEncrypter encrypter = new CharStreamEncrypter(encrypterFactory());
    for (int i = 0; i < 100; i++) {
      checkEncryptionDecryption(randomUnicodeOfCodepointLengthBetween(1, 10000), encrypter);
    }
  }

  private void checkEncryptionDecryption(String inputString, CharStreamEncrypter encrypter)
    throws IOException {

    // AES key length can either 16, 24 or 32 bytes.
    byte[] key = randomBytesOfLength(randomIntBetween(2, 4) * 8);

    // Encrypt the input string.
    StringBuilder encryptedBuilder = new StringBuilder();
    encrypter.encrypt(inputString, key, encryptedBuilder);

    // Decrypt the encrypted string.
    StringBuilder decryptedBuilder = new StringBuilder();
    encrypter.decrypt(encryptedBuilder.toString(), key, decryptedBuilder);
    assertEquals(inputString, decryptedBuilder.toString());
  }

  private AesCtrEncrypterFactory encrypterFactory() {
    if (LightAesCtrEncrypter.isSupported()) {
      return randomBoolean() ? CipherAesCtrEncrypter.FACTORY : LightAesCtrEncrypter.FACTORY;
    }
    return CipherAesCtrEncrypter.FACTORY;
  }
}
