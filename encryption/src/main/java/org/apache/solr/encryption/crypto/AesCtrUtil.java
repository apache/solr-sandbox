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

import java.security.SecureRandom;

/**
 * Methods for AES/CTR encryption.
 */
public class AesCtrUtil {

  // Rationale about the choice of the CTR-Mode:
  // - simple, efficient, random-access.
  // - adapted to Lucene immutable index files.
  // - file integrity and error detection checks are verified by Lucene checksums.
  // - nonce-misuse resistance is implemented when building the random IV in this class (see generateRandomAesCtrIv).
  // - used in combination with a strong AES cipher.
  //
  // See https://csrc.nist.rip/groups/ST/toolkit/BCM/documents/proposedmodes/ctr/ctr-spec.pdf
  // Comparison between AES-XTS and AES-CTR
  // See https://crypto.stackexchange.com/questions/64556/aes-xts-vs-aes-ctr-for-write-once-storage?rq=1

  /**
   * AES block has a fixed length of 16 bytes (128 bits).
   */
  public static final int AES_BLOCK_SIZE = 16;

  /**
   * AES/CTR IV length. It is equal to {@link #AES_BLOCK_SIZE}. It is defined separately mainly for code clarity.
   */
  public static final int IV_LENGTH = AES_BLOCK_SIZE;
  /** CTR counter length. 5 bytes means we can encrypt files up to 2^(5 x 8) x 16 B = 17.5 TB */
  private static final int COUNTER_LENGTH = 5;
  private static final long COUNTER_MAX_VALUE = (1L << (COUNTER_LENGTH * Byte.SIZE)) - 1;

  /**
   * Checks a key for AES. Its length must be either 16, 24 or 32 bytes.
   * @return true
   * @throws IllegalArgumentException If the key length is invalid.
   */
  public static boolean checkAesKey(byte[] key) {
    if (key.length != 16 && key.length != 24 && key.length != 32) {
      // AES requires either 128, 192 or 256 bits keys.
      throw new IllegalArgumentException("Invalid AES key length " + key.length + "; it must be either 128, 192 or 256 bits long");
    }
    return true;
  }

  /**
   * Checks the CTR counter. It must be greater than or equal to 0, and less than or equal to
   * {@link #COUNTER_MAX_VALUE}.
   * @return true
   * @throws IllegalArgumentException If the counter is invalid.
   */
  public static boolean checkCtrCounter(long counter) {
    if (counter < 0 || counter > COUNTER_MAX_VALUE) {
      throw new IllegalArgumentException("Invalid counter=" + counter);
    }
    return true;
  }

  /**
   * Generates a random IV for AES/CTR of length {@link #IV_LENGTH}.
   */
  public static byte[] generateRandomAesCtrIv(SecureRandom secureRandom) {
    // The IV length must be the AES block size.
    // For the CTR mode, the IV is composed of a random NONCE (first bytes) and a counter (last bytes).
    // com.sun.crypto.provider.CounterMode.increment() increments the counter starting from the last byte.

    // Rationale for the IV construction:
    // We have to ensure the IV is not repeated for the same encryption key. This is the (IV,key) pair
    // reuse problem. Otherwise, a crib-dragging technique could eliminate the confidentiality of the
    // paired contents (this would not reveal the encryption key).
    // One approach could be to use part of the IV bytes to encode the segment id. But the segment id has
    // a potentially unlimited size. And we would still need to differentiate the IV of the files in the
    // segment.
    // The approach taken here is a nonce-misuse resistance, to rely on a near-0 probability of having two
    // files sharing the same IV. We use all the remaining IV 11 bytes to generate 88 secure random bits
    // nounce.
    // The probability of having at least two files sharing the same IV is calculated here for 3 scenarios.
    // The probability can be estimated by following the Birthday Problem resolution
    // (https://en.wikipedia.org/wiki/Birthday_problem).
    // with n the number of files/IVs, and d the number of possible nounce values with 11 bytes (2^88), then
    // with n << d, the probability of having at least two identical IVs can be estimated by
    // P(n,d) ~= 1 - exp(- n(n-1)/2d) ~= n²/2d
    //
    // Scenario 1 typical - less than 2000 segments files on disk for the index.
    // Lucene frequently removes old unused segment files. Even if it takes some time to remove them on a
    // busy machine, we can consider there are less than 100 segments for an index on disk, which gives
    // less than 2000 files. This would be less with compound files.
    // P(2000,2^88) ~= 2000²/2^89 ~= 6.5E-21
    //
    // Scenario 2 very bad case - the key is changed every 4 months, all the segments produced are recorded
    // by an attacker, one commit/segment per 5 seconds, and 20 files per segment.
    // Num files recorded = 4*30*24*60*60/5*20 ~= 4.14E7 files
    // P(4.14E7, 2^88) ~= 2.7E-12
    //
    // Scenario 3 awfully bad case - the key is changed every year, all the segments produced are recorded
    // by an attacker, one commit/segment per second, and 20 files per segment.
    // Num files recorded = 365*24*60*60*20 ~= 6.3E8 files
    // P(6.3E8, 2^88) ~= 6.4E-10

    byte[] nonce = new byte[IV_LENGTH - COUNTER_LENGTH];
    secureRandom.nextBytes(nonce);
    byte[] iv = new byte[IV_LENGTH];
    System.arraycopy(nonce, 0, iv, 0, nonce.length);
    return iv;
  }

  /**
   * Builds an AES/CTR IV based on the provided counter and an initial IV.
   * The built IV is the same as with {@code com.sun.crypto.provider.CounterMode.increment()}.
   */
  public static void buildAesCtrIv(byte[] iv, long counter) {
    assert iv.length == IV_LENGTH;
    assert checkCtrCounter(counter);
    iv[iv.length - 1] = (byte) counter;
    iv[iv.length - 2] = (byte) (counter >>= 8);
    iv[iv.length - 3] = (byte) (counter >>= 8);
    iv[iv.length - 4] = (byte) (counter >>= 8);
    iv[iv.length - 5] = (byte) (counter >> 8);
  }
}