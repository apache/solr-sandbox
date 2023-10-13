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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Objects;

import static org.apache.solr.encryption.EncryptionTransactionLog.ENCRYPTION_KEY_HEADER_LENGTH;
import static org.apache.solr.encryption.EncryptionTransactionLog.readEncryptionHeader;
import static org.apache.solr.encryption.EncryptionTransactionLog.writeEncryptionHeader;
import static org.apache.solr.encryption.EncryptionUtil.getActiveKeyRefFromCommit;

/**
 * {@link UpdateLog} that creates {@link EncryptionUpdateLog} to encrypts logs if the
 * core index is marked for encryption.
 * <p>
 * The encryption is triggered when the {@link EncryptionRequestHandler} is called. It
 * commits with some metadata that mark the index for encryption. From that point, this
 * {@link EncryptionUpdateLog} will first re-encrypt old log files with the active
 * encryption key id (nearly as fast as a file copy), and it will encrypt new log files
 * with the same active key.
 */
public class EncryptionUpdateLog extends UpdateLog {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int REENCRYPTION_BUFFER_SIZE = 1024;

  private final DirectorySupplier directorySupplier = new DirectorySupplier();

  @Override
  public void init(UpdateHandler updateHandler, SolrCore core) {
    directorySupplier.init(core);
    super.init(updateHandler, core);
  }

  @Override
  public TransactionLog newTransactionLog(Path tlogFile, Collection<String> globalStrings, boolean openExisting) {
    if (openExisting) {
      assert Files.exists(tlogFile) : tlogFile + " does not exist";
      try {
        reencryptOldLogFileIfRequired(tlogFile);
      } catch (Exception e) {
        // Absorb the exception and continue opening the transaction log.
        log.error("Failure to re-encrypt log file (non fatal) {}", tlogFile, e);
      }
    }
    return new EncryptionTransactionLog(tlogFile, globalStrings, openExisting, directorySupplier);
  }

  protected void reencryptOldLogFileIfRequired(Path tlog) throws IOException {
    if (Files.size(tlog) > 0) {
      EncryptionDirectory directory = directorySupplier.get();
      try {
        directory.forceReadCommitUserData();
        try (FileChannel inputChannel = FileChannel.open(tlog, StandardOpenOption.READ)) {
          String inputKeyRef = readEncryptionHeader(inputChannel, ByteBuffer.allocate(4));
          String activeKeyRef = getActiveKeyRefFromCommit(directory.getLatestCommitData().data);
          if (!Objects.equals(inputKeyRef, activeKeyRef)) {
            Path newLog = tlog.resolveSibling(tlog.getFileName() + ".enc");
            try (OutputStream outputStream = new FileOutputStream(newLog.toFile())) {
              reencrypt(inputChannel, inputKeyRef, outputStream, activeKeyRef, directory);
            }
            Path backupLog = tlog.resolveSibling(tlog.getFileName() + ".bak");
            Files.move(tlog, backupLog);
            Files.move(newLog, tlog);
            Files.delete(backupLog);
          }
        }
      } finally {
        directorySupplier.release(directory);
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
      writeEncryptionHeader(activeKeyRef, outputStream);
      outputStream = new EncryptingOutputStream(outputStream,
                                                directory.getKeySecret(activeKeyRef),
                                                directory.getEncrypterFactory());
    }
    byte[] buffer = new byte[REENCRYPTION_BUFFER_SIZE];
    int nRead;
    while ((nRead = cfis.read(buffer, 0, buffer.length)) >= 0) {
      outputStream.write(buffer, 0, nRead);
    }
    outputStream.flush();
  }

  private static class DirectorySupplier implements EncryptionDirectorySupplier {

    private SolrCore core;

    void init(SolrCore core) {
      this.core = core;
    }

    @Override
    public EncryptionDirectory get() {
      try {
        return (EncryptionDirectory) EncryptionDirectoryFactory.getFactory(core, this)
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
