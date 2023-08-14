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
package org.apache.solr.encryption;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.encryption.crypto.AesCtrEncrypter;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.apache.solr.encryption.crypto.DecryptingIndexInput;
import org.apache.solr.encryption.crypto.EncryptingIndexOutput;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.solr.encryption.EncryptionUtil.*;

/**
 * {@link FilterDirectory} that wraps a delegate {@link Directory} to encrypt/decrypt files on the fly.
 * <p>
 * When opening an {@link IndexOutput} for writing:
 * <br>If {@link KeySupplier#shouldEncrypt(String)} returns true, and if there is an
 * {@link EncryptionUtil#getActiveKeyRefFromCommit(Map) active encryption key} defined in the latest
 * commit user data, then the output is wrapped with a {@link EncryptingIndexOutput} to be encrypted
 * on the fly. In this case an {@link #ENCRYPTION_MAGIC} header is written at the beginning of the output,
 * followed by the key reference number.
 * Otherwise, the {@link IndexOutput} created by the delegate is directly provided without encryption.
 * <p>
 * When opening an {@link IndexInput} for reading:
 * <br>If the input header is the {@link #ENCRYPTION_MAGIC}, then the key reference number that follows
 * is used to {@link EncryptionUtil#getKeyIdFromCommit get} the key id from the latest commit user data.
 * In this case the input is wrapped with a {@link DecryptingIndexInput} to be decrypted on the fly.
 * Otherwise, the {@link IndexInput} created by the delegate is directly provided without decryption.
 *
 * @see EncryptingIndexOutput
 * @see DecryptingIndexInput
 */
public class EncryptionDirectory extends FilterDirectory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Constant to identify the start of an encrypted file.
   * It is different from {@link CodecUtil#CODEC_MAGIC} to detect when a file is encrypted.
   */
  public static final int ENCRYPTION_MAGIC = 0x2E5BF271; // 777777777 in decimal

  protected final AesCtrEncrypterFactory encrypterFactory;

  protected final KeySupplier keySupplier;

  /** Cache of the latest commit user data. */
  protected volatile CommitUserData commitUserData;

  /** Optimization flag to avoid checking encryption when reading a file if we know the index is cleartext. */
  protected volatile boolean shouldCheckEncryptionWhenReading;

  /** Optimization flag to only read the commit user data once after a commit. */
  protected volatile boolean shouldReadCommitUserData;

  /**
   * Creates an {@link EncryptionDirectory} which wraps a delegate {@link Directory} to encrypt/decrypt
   * files on the fly.
   *
   * @param encrypterFactory creates {@link AesCtrEncrypter}.
   * @param keySupplier      provides the key secrets and determines which files should be encrypted.
   */
  public EncryptionDirectory(Directory delegate, AesCtrEncrypterFactory encrypterFactory, KeySupplier keySupplier)
    throws IOException {
    super(delegate);
    this.encrypterFactory = encrypterFactory;
    this.keySupplier = keySupplier;
    commitUserData = readLatestCommitUserData();

    // If there is no encryption key id parameter in the latest commit user data, then we know the index
    // is cleartext, so we can skip fast any encryption check. This flag becomes true indefinitely if we
    // detect an encryption key when opening a file for writing.
    shouldCheckEncryptionWhenReading = hasKeyIdInCommit(commitUserData.data);
  }

  @Override
  public IndexOutput createOutput(String fileName, IOContext context) throws IOException {
    return maybeWrapOutput(in.createOutput(fileName, context));
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    return maybeWrapOutput(in.createTempOutput(prefix, suffix, context));
  }

  /**
   * Maybe wraps the {@link IndexOutput} created by the delegate {@link Directory} with an
   * {@link EncryptingIndexOutput}.
   */
  protected IndexOutput maybeWrapOutput(IndexOutput indexOutput) throws IOException {
    String fileName = indexOutput.getName();
    assert !fileName.startsWith(IndexFileNames.SEGMENTS);
    if (fileName.startsWith(IndexFileNames.PENDING_SEGMENTS)) {
      // The pending_segments file should not be encrypted. Do not wrap the IndexOutput.
      // It also means a commit has started, so set the flag to read the commit user data
      // next time we need it.
      shouldReadCommitUserData = true;
      return indexOutput;
    }
    if (!keySupplier.shouldEncrypt(fileName)) {
      // The file should not be encrypted, based on its name. Do not wrap the IndexOutput.
      return indexOutput;
    }
    boolean success = false;
    try {
      String keyRef = getKeyRefForWriting(indexOutput);
      if (keyRef != null) {
        // The IndexOutput has to be wrapped to be encrypted with the key.
        indexOutput = new EncryptingIndexOutput(indexOutput, getKeySecret(keyRef), encrypterFactory);
      }
      success = true;
    } finally {
      if (!success) {
        // Something went wrong. Close the IndexOutput before the exception continues.
        IOUtils.closeWhileHandlingException(indexOutput);
      }
    }
    return indexOutput;
  }

  /**
   * Gets the active key reference number for writing an index output.
   * <p>
   * The active key ref is defined in the user data of the latest commit. If it is present, then this method
   * writes to the output the {@link #ENCRYPTION_MAGIC} header, followed by the key reference number as a
   * 4B big-endian int.
   *
   * @return the key reference number; or null if none.
   */
  protected String getKeyRefForWriting(IndexOutput indexOutput) throws IOException {
    String keyRef;
    if ((keyRef = getActiveKeyRefFromCommit(getLatestCommitData().data)) == null) {
      return null;
    }
    shouldCheckEncryptionWhenReading = true;
    // Write the encryption magic header and the key reference number.
    writeBEInt(indexOutput, ENCRYPTION_MAGIC);
    writeBEInt(indexOutput, Integer.parseInt(keyRef));
    return keyRef;
  }

  /** Write int value on header / footer with big endian order. See readBEInt. */
  private static void writeBEInt(DataOutput out, int i) throws IOException {
    out.writeByte((byte) (i >> 24));
    out.writeByte((byte) (i >> 16));
    out.writeByte((byte) (i >> 8));
    out.writeByte((byte) i);
  }

  /**
   * Gets the user data from the latest commit, potentially reading the latest commit if the cache is stale.
   */
  protected CommitUserData getLatestCommitData() throws IOException {
    if (shouldReadCommitUserData) {
      synchronized (this) {
        if (shouldReadCommitUserData) {
          CommitUserData newCommitUserData = readLatestCommitUserData();
          if (newCommitUserData != commitUserData) {
            commitUserData = newCommitUserData;
            shouldReadCommitUserData = false;
          }
        }
      }
    }
    return commitUserData;
  }

  /**
   * Reads the user data from the latest commit, or keeps the cached value if the segments file name has
   * not changed.
   */
  protected CommitUserData readLatestCommitUserData() throws IOException {
    try {
      return new SegmentInfos.FindSegmentsFile<CommitUserData>(this) {
        protected CommitUserData doBody(String segmentFileName) throws IOException {
          if (commitUserData != null && commitUserData.segmentFileName.equals(segmentFileName)) {
            // If the segments file is the same, then keep the same commit user data.
            return commitUserData;
          }
          // New segments file, so we have to read it.
          SegmentInfos segmentInfos = SegmentInfos.readCommit(EncryptionDirectory.this, segmentFileName);
          return createCommitUserData(segmentFileName, segmentInfos.getUserData());
        }
      }.run();
    } catch (NoSuchFileException | FileNotFoundException e) {
      // No commit yet, so no encryption key.
      return CommitUserData.EMPTY;
    }
  }

  protected CommitUserData createCommitUserData(String segmentFileName, Map<String, String> data) {
    return new CommitUserData(segmentFileName, data);
  }

  /**
   * Gets the key secret from the provided key reference number.
   * First, gets the key id corresponding to the key reference based on the mapping defined in the latest
   * commit user data. Then, calls the {@link KeySupplier} to get the corresponding key secret.
   */
  protected byte[] getKeySecret(String keyRef) throws IOException {
    String keyId = getKeyIdFromCommit(keyRef, getLatestCommitData().data);
    return keySupplier.getKeySecret(keyId, this::getKeyCookie);
  }

  /**
   * Gets the key cookie to provide to the {@link KeySupplier} to get the key secret.
   *
   * @return the key cookie key-value pairs; or null if none.
   */
  @Nullable
  protected Map<String, String> getKeyCookie(String keyId) {
    return commitUserData.keyCookies.get(keyId);
  }

  @Override
  public IndexInput openInput(String fileName, IOContext context) throws IOException {
    IndexInput indexInput = in.openInput(fileName, context);
    if (!shouldCheckEncryptionWhenReading) {
      // Return the IndexInput directly as we know it is not encrypted.
      return indexInput;
    }
    boolean success = false;
    try {
      String keyRef = getKeyRefForReading(indexInput);
      if (keyRef != null) {
        // The IndexInput has to be wrapped to be decrypted with the key.
        indexInput = new DecryptingIndexInput(indexInput, getKeySecret(keyRef), encrypterFactory);
      }
      success = true;
    } finally {
      if (!success) {
        // Something went wrong. Close the IndexInput before the exception continues.
        IOUtils.closeWhileHandlingException(indexInput);
      }
    }
    return indexInput;
  }

  /**
   * Gets the key reference number for reading an index input.
   * <p>
   * If the file is ciphered, it starts with the {@link #ENCRYPTION_MAGIC} header, followed by the reference
   * number as a 4B big-endian int.
   * If the file is cleartext, it starts with the {@link CodecUtil#CODEC_MAGIC} header.
   *
   * @return the key reference number; or null if none.
   */
  protected String getKeyRefForReading(IndexInput indexInput) throws IOException {
    long filePointer = indexInput.getFilePointer();
    int magic = readBEInt(indexInput);
    if (magic == ENCRYPTION_MAGIC) {
      // This file is encrypted.
      // Read the key reference that follows.
      return Integer.toString(readBEInt(indexInput));
    } else {
      // This file is cleartext.
      // Restore the file pointer.
      indexInput.seek(filePointer);
      return null;
    }
  }

  /**
   * Read int value from header / footer with big endian order.
   * We force big endian order when reading a codec. See CodecUtil.readBEInt in Lucene 9.0 or above.
   */
  private static int readBEInt(DataInput in) throws IOException {
    return ((in.readByte() & 0xFF) << 24)
      | ((in.readByte() & 0xFF) << 16)
      | ((in.readByte() & 0xFF) << 8)
      | (in.readByte() & 0xFF);
  }

  /**
   * Returns the segments having an encryption key id different from the active one.
   *
   * @param activeKeyId the current active key id, or null if none.
   * @return the segments with old key ids, or an empty list if none.
   */
  public List<SegmentCommitInfo> getSegmentsWithOldKeyId(SegmentInfos segmentInfos, String activeKeyId)
    throws IOException {
    List<SegmentCommitInfo> segmentsWithOldKeyId = null;
    if (log.isDebugEnabled()) {
      log.debug("reading segments {} for key ids different from {}",
                segmentInfos.asList().stream().map(i -> i.info.name).collect(Collectors.toList()),
                activeKeyId);
    }
    for (SegmentCommitInfo segmentCommitInfo : segmentInfos) {
      for (String fileName : segmentCommitInfo.files()) {
        if (keySupplier.shouldEncrypt(fileName)) {
          try (IndexInput fileInput = in.openInput(fileName, IOContext.READ)) {
            String keyRef = getKeyRefForReading(fileInput);
            String keyId = keyRef == null ? null : getKeyIdFromCommit(keyRef, segmentInfos.getUserData());
            log.debug("reading file {} of segment {} => keyId={}", fileName, segmentCommitInfo.info.name, keyId);
            if (!Objects.equals(keyId, activeKeyId)) {
              if (segmentsWithOldKeyId == null) {
                segmentsWithOldKeyId = new ArrayList<>();
              }
              segmentsWithOldKeyId.add(segmentCommitInfo);
            }
          }
          break;
        }
      }
    }
    return segmentsWithOldKeyId == null ? Collections.emptyList() : segmentsWithOldKeyId;
  }

  /**
   * Keeps the {@link SegmentInfos commit} file name and user data.
   */
  protected static class CommitUserData {

    protected static final CommitUserData EMPTY = new CommitUserData("", Collections.emptyMap());

    protected final String segmentFileName;
    protected final Map<String, String> data;
    protected final KeyCookies keyCookies;

    protected CommitUserData(String segmentFileName, Map<String, String> data) {
      this.segmentFileName = segmentFileName;
      this.data = data;
      keyCookies = getKeyCookiesFromCommit(data);
    }
  }
}
