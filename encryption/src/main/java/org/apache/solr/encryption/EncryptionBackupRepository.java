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
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.backup.repository.DelegatingBackupRepository;

import java.io.IOException;
import java.net.URI;

import static org.apache.solr.core.backup.repository.AbstractBackupRepository.PARAM_VERIFY_CHECKSUM;

/**
 * Encryption {@link org.apache.solr.core.backup.repository.BackupRepository} that delegates
 * to another {@link org.apache.solr.core.backup.repository.BackupRepository}.
 * It ensures the encrypted files are copied encrypted, but still verifies their checksum before the copy.
 * It also ensures the files are copied encrypted when restored (not double encrypted).
 */
public class EncryptionBackupRepository extends DelegatingBackupRepository {

  protected boolean shouldVerifyChecksum = true;

  @Override
  public void init(NamedList<?> args) {
    shouldVerifyChecksum = getBooleanConfig(args, PARAM_VERIFY_CHECKSUM, true);
    if (delegate != null) {
      delegate.init(args);
    }
  }

  @Override
  protected NamedList<?> getDelegateInitArgs(NamedList<?> initArgs) {
    // Enable checksum verification only if both this repository and its delegate enable it.
    shouldVerifyChecksum &= getBooleanConfig(initArgs, PARAM_VERIFY_CHECKSUM, true);

    // Ensure the delegate does not verify the checksum, otherwise it would fail since we back up encrypted files.
    NamedList<Object> newInitArgs = new NamedList<>(initArgs.size() + 1);
    newInitArgs.add(PARAM_VERIFY_CHECKSUM, Boolean.FALSE.toString());
    newInitArgs.addAll(initArgs);
    return newInitArgs;
  }

  @Override
  public void copyIndexFileFrom(
          Directory sourceDir, String sourceFileName, Directory destDir, String destFileName)
          throws IOException {
    if (shouldVerifyChecksum) {
      // Read and verify the checksum with the potentially decrypting directory.
      verifyChecksum(sourceDir, sourceFileName);
    }
    // Copy the index file with the unwrapped (delegate) directory to keep encryption.
    super.copyIndexFileFrom(FilterDirectory.unwrap(sourceDir), sourceFileName, destDir, destFileName);
  }

  @Override
  public void copyIndexFileFrom(
          Directory sourceDir, String sourceFileName, URI destUri, String destFileName)
          throws IOException {
    if (shouldVerifyChecksum) {
      // Read and verify the checksum with the potentially decrypting directory.
      verifyChecksum(sourceDir, sourceFileName);
    }
    // Copy the index file with the unwrapped (delegate) directory to keep encryption.
    super.copyIndexFileFrom(FilterDirectory.unwrap(sourceDir), sourceFileName, destUri, destFileName);
  }

  private void verifyChecksum(Directory sourceDir, String sourceFileName) throws IOException {
    try (ChecksumIndexInput is = sourceDir.openChecksumInput(sourceFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
      long left = is.length() - CodecUtil.footerLength();
      long bufferSize = 8192;
      byte[] buffer = new byte[(int) bufferSize];
      while (left > 0) {
        int toCopy = (int) Math.min(left, bufferSize);
        is.readBytes(buffer, 0, toCopy);
        left -= toCopy;
      }
      // Verify the checksum.
      CodecUtil.checkFooter(is);
    }
  }

  @Override
  public void copyIndexFileTo(
          URI sourceRepo, String sourceFileName, Directory destDir, String destFileName)
          throws IOException {
    // Copy the index file with the unwrapped (delegate) directory to avoid encrypting twice.
    super.copyIndexFileTo(sourceRepo, sourceFileName, FilterDirectory.unwrap(destDir), destFileName);
  }

  protected static boolean getBooleanConfig(NamedList<?> args, String param, boolean defaultValue) {
    Object value = args.get(param);
    return value == null ? defaultValue : Boolean.parseBoolean(value.toString());
  }
}
