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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;
import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;
import static org.apache.solr.encryption.crypto.AesCtrUtil.*;

/**
 * Tests {@link EncryptingIndexOutput}.
 */
public class EncryptingIndexOutputTest extends BaseDataOutputTestCase<EncryptingIndexOutput> {

  private byte[] key;
  private boolean shouldSimulateWrongKey;

  @Before
  public void initializeEncryption() {
    // AES key length can either 16, 24 or 32 bytes.
    key = randomBytesOfLength(randomIntBetween(2, 4) * 8);
    shouldSimulateWrongKey = false;
  }

  /**
   * Verifies that the length of the encrypted output is the same as the original data.
   * Verifies that all the file pointers in the encrypted output are the same as the original data pointers.
   */
  @Test
  public void testEncryptionLength() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamIndexOutput delegateIndexOutput = new OutputStreamIndexOutput("test", "test", baos, 10);
    byte[] key = new byte[32];
    Arrays.fill(key, (byte) 1);
    // Reduce the buffer capacity to make sure we often write to the index output.
    EncryptingIndexOutput indexOutput = new EncryptingIndexOutput(delegateIndexOutput, key, encrypterFactory(), AesCtrUtil.AES_BLOCK_SIZE);
    indexOutput.writeByte((byte) 3);
    // Check same file pointer.
    assertEquals(1, indexOutput.getFilePointer());
    byte[] bytes = "tomorrow morning".getBytes(StandardCharsets.UTF_16);
    indexOutput.writeBytes(bytes, 0, bytes.length);
    // Check same file pointer.
    assertEquals(1 + bytes.length, indexOutput.getFilePointer());
    indexOutput.close();
    // Check the output size is equal to the original size + IV length.
    assertEquals(1 + bytes.length + IV_LENGTH, baos.size());
  }

  /**
   * Verify that with a wrong key we don't get the original data when decrypting.
   */
  @Test
  public void testWrongKey() {
    shouldSimulateWrongKey = true;
    // Run the testRandomizedWrites which encrypts data and then calls
    // toBytes() which decrypts with a wrong key.
    LuceneTestCase.expectThrows(AssertionError.class, this::testRandomizedWrites);
  }

  @Override
  protected EncryptingIndexOutput newInstance() {
    try {
      return new TestBufferedEncryptingIndexOutput(new ByteBuffersDataOutput(), key, encrypterFactory());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected byte[] toBytes(EncryptingIndexOutput indexOutput) {
    try {
      indexOutput.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    ByteBuffersDataInput dataInput = ((TestBufferedEncryptingIndexOutput) indexOutput).dataOutput.toDataInput();
    IndexInput indexInput = new ByteBuffersIndexInput(dataInput, "Test");
    byte[] key = this.key.clone();
    if (shouldSimulateWrongKey) {
      key[0]++;
    }
    try (DecryptingIndexInput decryptingIndexInput = new DecryptingIndexInput(indexInput, key, encrypterFactory())) {
      byte[] b = new byte[(int) decryptingIndexInput.length()];
      decryptingIndexInput.readBytes(b, 0, b.length);
      return b;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private AesCtrEncrypterFactory encrypterFactory() {
    return randomBoolean() ? CipherAesCtrEncrypter.FACTORY : LightAesCtrEncrypter.FACTORY;
  }

  /**
   * Replaces the {@link java.security.SecureRandom} by a repeatable {@link Random} for tests.
   * This is used to generate a repeatable random IV.
   */
  private static class TestBufferedEncryptingIndexOutput extends EncryptingIndexOutput {

    private final ByteBuffersDataOutput dataOutput;

    TestBufferedEncryptingIndexOutput(ByteBuffersDataOutput dataOutput, byte[] key, AesCtrEncrypterFactory encrypterFactory) throws IOException {
      super(new ByteBuffersIndexOutput(dataOutput, "Test", "Test"), key, encrypterFactory);
      this.dataOutput = dataOutput;
    }

    @Override
    protected byte[] generateRandomIv() {
      return randomBytesOfLength(IV_LENGTH);
    }
  }
}