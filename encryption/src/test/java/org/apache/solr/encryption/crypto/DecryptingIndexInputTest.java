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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOConsumer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.encryption.crypto.CryptoTestUtil.encrypterFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link DecryptingIndexInput}.
 */
public class DecryptingIndexInputTest extends RandomizedTest {

  private byte[] key;

  @Before
  public void initializeEncryption() {
    // AES key length can either 16, 24 or 32 bytes.
    key = randomBytesOfLength(randomIntBetween(2, 4) * 8);
  }

  @Test
  public void testSanity() throws IOException {
    ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
    EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);
    indexOutput.close();
    DecryptingIndexInput indexInput1 = createDecryptingIndexInput(dataOutput, 0);
    assertEquals(0, indexInput1.length());
    LuceneTestCase.expectThrows(EOFException.class, indexInput1::readByte);

    dataOutput = new ByteBuffersDataOutput();
    indexOutput = createEncryptingIndexOutput(dataOutput);
    indexOutput.writeByte((byte) 1);
    indexOutput.close();

    DecryptingIndexInput indexInput2 = createDecryptingIndexInput(dataOutput, 0);
    assertEquals(1, indexInput2.length());
    assertEquals(0, indexInput2.getFilePointer());
    assertEquals(0, indexInput1.length());
    indexInput1.close();

    assertEquals(1, indexInput2.readByte());
    assertEquals(1, indexInput2.getFilePointer());
    assertEquals(1, indexInput2.randomAccessSlice(0, 1).readByte(0));

    LuceneTestCase.expectThrows(EOFException.class, indexInput2::readByte);

    assertEquals(1, indexInput2.getFilePointer());
    indexInput2.close();
  }

  @Test
  public void testRandomReads() throws Exception {
    ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
    int offset = writeRandomBytes(dataOutput);
    EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

    long seed = randomLong();
    int maxAddCalls = 100_000;
    List<IOConsumer<DataInput>> reply =
      BaseDataOutputTestCase.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), maxAddCalls);
    indexOutput.close();

    DecryptingIndexInput indexInput = createDecryptingIndexInput(dataOutput, offset);
    for (IOConsumer<DataInput> c : reply) {
      c.accept(indexInput);
    }

    LuceneTestCase.expectThrows(EOFException.class, indexInput::readByte);
    indexInput.close();
  }

  @Test
  public void testRandomReadsOnSlices() throws Exception {
    for (int reps = randomIntBetween(1, 20); --reps > 0; ) {
      ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
      int offset = writeRandomBytes(dataOutput);
      EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

      byte[] prefix = new byte[randomIntBetween(0, 1024 * 8)];
      indexOutput.writeBytes(prefix, 0, prefix.length);

      long seed = randomLong();
      int max = 10_000;
      List<IOConsumer<DataInput>> reply =
        BaseDataOutputTestCase.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), max);

      byte[] suffix = new byte[randomIntBetween(0, 1024 * 8)];
      indexOutput.writeBytes(suffix, 0, suffix.length);
      long outputLength = indexOutput.getFilePointer();
      indexOutput.close();

      try (IndexInput indexInput = createDecryptingIndexInput(dataOutput, offset)) {
        IndexInput sliceInput = indexInput.slice("Test", prefix.length, outputLength - prefix.length - suffix.length);

        assertEquals(0, sliceInput.getFilePointer());
        assertEquals(outputLength - prefix.length - suffix.length, sliceInput.length());
        for (IOConsumer<DataInput> c : reply) {
          c.accept(sliceInput);
        }

        LuceneTestCase.expectThrows(EOFException.class, sliceInput::readByte);
      }
    }
  }

  @Test
  public void testSeekEmpty() throws Exception {
    ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
    createEncryptingIndexOutput(dataOutput).close();
    DecryptingIndexInput indexInput = createDecryptingIndexInput(dataOutput, 0);

    indexInput.seek(0);
    LuceneTestCase.expectThrows(EOFException.class, () -> indexInput.seek(1));

    indexInput.seek(0);
    LuceneTestCase.expectThrows(EOFException.class, indexInput::readByte);
    indexInput.close();
  }

  @Test
  public void testSeek() throws Exception {
    for (int reps = randomIntBetween(1, 200); --reps > 0; ) {
      ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
      int offset = writeRandomBytes(dataOutput);
      ByteBuffersDataOutput clearDataOutput = new ByteBuffersDataOutput();
      EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

      byte[] prefix = {};
      if (randomBoolean()) {
        prefix = new byte[randomIntBetween(1, 1024 * 8)];
        indexOutput.writeBytes(prefix, 0, prefix.length);
        clearDataOutput.writeBytes(prefix);
      }

      long seed = randomLong();
      int max = 1000;
      List<IOConsumer<DataInput>> reply =
        BaseDataOutputTestCase.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), max);
      BaseDataOutputTestCase.addRandomData(clearDataOutput, new Xoroshiro128PlusRandom(seed), max);
      assertEquals(clearDataOutput.size(), indexOutput.getFilePointer());
      long outputLength = indexOutput.getFilePointer();
      indexOutput.close();

      try (IndexInput indexInput = createDecryptingIndexInput(dataOutput, offset)) {
        IndexInput sliceInput = indexInput.slice("Test", prefix.length, outputLength - prefix.length);

        sliceInput.seek(0);
        for (IOConsumer<DataInput> c : reply) {
          c.accept(sliceInput);
        }

        sliceInput.seek(0);
        for (IOConsumer<DataInput> c : reply) {
          c.accept(sliceInput);
        }

        byte[] clearData = clearDataOutput.toArrayCopy();
        clearData = ArrayUtil.copyOfSubArray(clearData, prefix.length, clearData.length);

        for (int i = 0; i < 1000; i++) {
          int offs = randomIntBetween(0, clearData.length - 1);
          sliceInput.seek(offs);
          assertEquals(offs, sliceInput.getFilePointer());
          assertEquals("reps=" + reps + " i=" + i + ", offs=" + offs, clearData[offs], sliceInput.readByte());
        }
        sliceInput.seek(sliceInput.length());
        assertEquals(sliceInput.length(), sliceInput.getFilePointer());
        LuceneTestCase.expectThrows(EOFException.class, sliceInput::readByte);
      }
    }
  }

  @Test
  public void testClone() throws Exception {
    for (int reps = randomIntBetween(1, 200); --reps > 0; ) {
      ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
      int offset = writeRandomBytes(dataOutput);
      ByteBuffersDataOutput clearDataOutput = new ByteBuffersDataOutput();
      EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

      long seed = randomLong();
      int max = 1000;
      BaseDataOutputTestCase.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), max);
      BaseDataOutputTestCase.addRandomData(clearDataOutput, new Xoroshiro128PlusRandom(seed), max);
      assertEquals(clearDataOutput.size(), indexOutput.getFilePointer());
      indexOutput.close();

      IndexInput indexInput = createDecryptingIndexInput(dataOutput, offset);
      byte[] clearData = clearDataOutput.toArrayCopy();
      byte[] readBuffer = new byte[100];
      for (int i = 0; i < 1000; i++) {
        int readLength = randomIntBetween(1, readBuffer.length);
        int offs = randomIntBetween(0, clearData.length - 1 - 2 * readLength);
        indexInput.seek(offs);
        assertEquals("reps=" + reps + " i=" + i + ", offs=" + offs, clearData[offs], indexInput.readByte());
        indexInput.readBytes(readBuffer, 0, readLength);
        assertTrue(Arrays.equals(clearData, offs + 1, offs + 1 + readLength, readBuffer, 0, readLength));

        IndexInput clone = indexInput.clone();
        if (randomBoolean()) {
          clone.readBytes(readBuffer, 0, readLength);
          assertTrue(Arrays.equals(clearData, offs + 1 + readLength, offs + 1 + 2 * readLength, readBuffer, 0, readLength));
        }
        int cloneOffs = Math.max(offs - readLength + randomIntBetween(0, 2 * readLength), 0);
        clone.seek(cloneOffs);
        clone.readBytes(readBuffer, 0, readLength);
        assertTrue(Arrays.equals(clearData, cloneOffs, cloneOffs + readLength, readBuffer, 0, readLength));
        clone.close();
      }
      indexInput.close();
    }
  }

  private int writeRandomBytes(DataOutput dataOutput) throws IOException {
    int numBytes = randomIntBetween(0, 10);
    for (int i = 0; i < numBytes; i++) {
      dataOutput.writeByte((byte) 1);
    }
    return numBytes;
  }

  private EncryptingIndexOutput createEncryptingIndexOutput(ByteBuffersDataOutput dataOutput) throws IOException {
    return new EncryptingIndexOutput(new ByteBuffersIndexOutput(dataOutput, "Test", "Test"),
      key, encrypterFactory());
  }

  private DecryptingIndexInput createDecryptingIndexInput(ByteBuffersDataOutput dataOutput, int offset) throws IOException {
    IndexInput indexInput = new ByteBuffersIndexInput(dataOutput.toDataInput(), "Test");
    indexInput.seek(offset);
    return new DecryptingIndexInput(indexInput, key, encrypterFactory());
  }
}