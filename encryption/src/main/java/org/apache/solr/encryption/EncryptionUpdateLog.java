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

import org.apache.solr.common.SolrException;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.encryption.EncryptionTransactionLog.EncryptionDirectorySupplier;
import org.apache.solr.encryption.crypto.DecryptingChannelInputStream;
import org.apache.solr.encryption.crypto.EncryptingOutputStream;
import org.apache.solr.update.TransactionLog;
import org.apache.solr.update.TransactionLog.ChannelFastInputStream;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.solr.encryption.EncryptionTransactionLog.ENCRYPTION_KEY_HEADER_LENGTH;
import static org.apache.solr.encryption.EncryptionTransactionLog.readEncryptionHeader;
import static org.apache.solr.encryption.EncryptionTransactionLog.writeEncryptionHeader;
import static org.apache.solr.encryption.EncryptionUtil.getActiveKeyRefFromCommit;
import static org.apache.solr.encryption.EncryptionUtil.getKeyIdFromCommit;

/**
 * {@link UpdateLog} that creates {@link EncryptionUpdateLog} to encrypts logs if the
 * core index is marked for encryption.
 * <p>
 * The encryption is triggered when the {@link EncryptionRequestHandler} is called. It
 * commits with some metadata that mark the index for encryption. It also calls
 * {@link #encryptLogs} to (re)encrypt old log files with the active encryption key id
 * (nearly as fast as a file copy). New log files created with {@link #newTransactionLog}
 * will be encrypted using the active key stored in the commit metadata.
 */
public class EncryptionUpdateLog extends UpdateLog {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int REENCRYPTION_BUFFER_SIZE = 1024;

  protected final DirectorySupplier directorySupplier = new DirectorySupplier();

  protected boolean shouldEncryptOldLogs = true;

  @Override
  public void init(UpdateHandler updateHandler, SolrCore core) {
    directorySupplier.init(core);
    super.init(updateHandler, core);
    try {
      encryptLogs();
    } catch (IOException e) {
      log.error("Exception while encrypting old transaction logs", e);
    }
  }

  @Override
  public TransactionLog newTransactionLog(Path tlogFile, Collection<String> globalStrings, boolean openExisting) {
    return new EncryptionTransactionLog(tlogFile, globalStrings, openExisting, directorySupplier);
  }

  /**
   * (Re)encrypts all existing transaction logs if an encryption key is defined in the {@link SolrCore}
   * index commit metadata.
   *
   * @return {@code true} if all logs were successfully encrypted; {@code false} if some logs were not
   * encrypted because they are still in use, in this case the encryption will be retried at the next
   * commit when {@link #addOldLog} is called.
   */
  public synchronized boolean encryptLogs() throws IOException {
    Map<String, String> latestCommitData;
    String activeKeyRef;
    EncryptionDirectory directory = directorySupplier.get();
    try {
      directory.clearCachedCommitUserData();
      latestCommitData = directory.getLatestCommitData().data;
      activeKeyRef = getActiveKeyRefFromCommit(latestCommitData);
      for (TransactionLog log : logs) {
        EncryptionTransactionLog encLog = (EncryptionTransactionLog) log;
        if (encLog.refCount() <= 1) {
          // The log is only owned by this update log. We can encrypt it.
          encryptLog(encLog, activeKeyRef, directory);
        }
      }
    } finally {
      directorySupplier.release(directory);
    }
    String keyId = activeKeyRef == null ? null : getKeyIdFromCommit(activeKeyRef, latestCommitData);
    boolean allLogsEncrypted = areAllLogsEncryptedWithKeyId(keyId, latestCommitData);
    shouldEncryptOldLogs = !allLogsEncrypted;
    return allLogsEncrypted;
  }

  @Override
  protected synchronized void addOldLog(TransactionLog oldLog, boolean removeOld) {
    super.addOldLog(oldLog, removeOld);
    if (shouldEncryptOldLogs) {
      try {
        encryptLogs();
      } catch (IOException e) {
        log.error("Exception while encrypting old transaction logs", e);
      }
    }
  }

  /**
   * Returns whether all logs are encrypted with the provided key id.
   */
  public synchronized boolean areAllLogsEncryptedWithKeyId(String keyId, Map<String, String> commitUserData)
          throws IOException {
    List<TransactionLog> allLogs = getAllLogs();
    if (!allLogs.isEmpty()) {
      for (TransactionLog log : allLogs) {
        try (FileChannel logChannel = FileChannel.open(((EncryptionTransactionLog) log).path(), StandardOpenOption.READ)) {
          String logKeyRef = readEncryptionHeader(logChannel);
          String logKeyId = logKeyRef == null ? null : getKeyIdFromCommit(logKeyRef, commitUserData);
          if (!Objects.equals(logKeyId, keyId)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private List<TransactionLog> getAllLogs() {
    List<TransactionLog> allLogs = new ArrayList<>(logs.size() + 3);
    if (tlog != null) {
      allLogs.add(tlog);
    }
    if (prevTlog != null) {
      allLogs.add(prevTlog);
    }
    if (bufferTlog != null) {
      allLogs.add(bufferTlog);
    }
    allLogs.addAll(logs);
    return allLogs;
  }

  /**
   * Encrypts a transaction log if it is not encrypted with the latest encryption key.
   * The transaction log is toggled in readonly mode to ensure it is not written anymore.
   */
  protected void encryptLog(EncryptionTransactionLog log, String activeKeyRef, EncryptionDirectory directory)
          throws IOException {
    assert log.refCount() <= 1;
    if (Files.size(log.path()) > 0) {
      try (FileChannel inputChannel = FileChannel.open(log.path(), StandardOpenOption.READ)) {
        String inputKeyRef = readEncryptionHeader(inputChannel);
        if (!Objects.equals(inputKeyRef, activeKeyRef)) {
          Path newLogPath = log.path().resolveSibling(log.path().getFileName() + ".enc");
          try (OutputStream outputStream = Files.newOutputStream(newLogPath, StandardOpenOption.CREATE)) {
            reencrypt(inputChannel, inputKeyRef, outputStream, activeKeyRef, directory);
          }
          Path backupLog = log.path().resolveSibling(log.path().getFileName() + ".bak");
          Files.move(log.path(), backupLog, StandardCopyOption.REPLACE_EXISTING);
          Files.move(newLogPath, log.path(), StandardCopyOption.REPLACE_EXISTING);
          Files.delete(backupLog);
          // Toggle to readonly mode to make sure we don't write anymore to the transaction
          // log output stream. Reading the transaction log is ok because it will open new
          // input streams.
          log.readOnly();
        }
      }
    }
  }

  protected void reencrypt(FileChannel inputChannel,
                         String inputKeyRef,
                         OutputStream outputStream,
                         String activeKeyRef,
                         EncryptionDirectory directory)
    throws IOException {
    ChannelFastInputStream cfis = inputKeyRef == null ?
      new ChannelFastInputStream(inputChannel, 0)
      : new DecryptingChannelInputStream(inputChannel,
                                         ENCRYPTION_KEY_HEADER_LENGTH,
                                         0,
                                         directory.getKeySecret(inputKeyRef),
                                         directory.getEncrypterFactory());
    if (activeKeyRef != null) {
      // Get the key secret first. If it fails, we do not write anything.
      byte[] keySecret = directory.getKeySecret(activeKeyRef);
      writeEncryptionHeader(activeKeyRef, outputStream);
      outputStream = new EncryptingOutputStream(outputStream,
                                                keySecret,
                                                directory.getEncrypterFactory());
    }
    byte[] buffer = new byte[REENCRYPTION_BUFFER_SIZE];
    int nRead;
    while ((nRead = cfis.read(buffer, 0, buffer.length)) >= 0) {
      outputStream.write(buffer, 0, nRead);
    }
    outputStream.flush();
  }

  protected static class DirectorySupplier implements EncryptionDirectorySupplier {

    private SolrCore core;

    void init(SolrCore core) {
      this.core = core;
    }

    @Override
    public EncryptionDirectory get() {
      try {
        return (EncryptionDirectory) EncryptionDirectoryFactory.getFactory(core)
          .get(core.getIndexDir(),
               DirectoryFactory.DirContext.DEFAULT,
               core.getSolrConfig().indexConfig.lockType);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot encrypt tlogs", e);
      }
    }

    @Override
    public void release(EncryptionDirectory directory) throws IOException {
      core.getDirectoryFactory().release(directory);
    }
  }
}
