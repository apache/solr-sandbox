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

import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.apache.solr.encryption.crypto.DecryptingChannelInputStream;
import org.apache.solr.encryption.crypto.EncryptingOutputStream;
import org.apache.solr.update.TransactionLog;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;

import static org.apache.solr.encryption.EncryptionDirectory.ENCRYPTION_MAGIC;
import static org.apache.solr.encryption.EncryptionUtil.getActiveKeyRefFromCommit;
import static org.apache.solr.encryption.crypto.AesCtrUtil.IV_LENGTH;

/**
 * {@link TransactionLog} that encrypts logs if the core index is marked for encryption.
 * <p>
 * It decrypts old logs based on the key id stored in the header of each log file. It
 * encrypts new log files with the latest active key id defined in the core index metadata.
 * It appends to an existing log file using the same key id stored in the header of the file.
 * <p>
 * If the core index is not marked for encryption, it writes new log files in cleartext.
 */
public class EncryptionTransactionLog extends TransactionLog {

  protected static final int ENCRYPTION_KEY_HEADER_LENGTH = 2 * Integer.BYTES;
  protected static final int ENCRYPTION_FULL_HEADER_LENGTH = ENCRYPTION_KEY_HEADER_LENGTH + IV_LENGTH;

  /** Creates an {@link EncryptionTransactionLog}. */
  public EncryptionTransactionLog(Path tlogFile,
                                  Collection<String> globalStrings,
                                  boolean openExisting,
                                  EncryptionDirectorySupplier directorySupplier) {
    this(tlogFile, globalStrings, openExisting, directorySupplier, new IvHolder());
  }

  protected EncryptionTransactionLog(Path tlogFile,
                                   Collection<String> globalStrings,
                                   boolean openExisting,
                                   EncryptionDirectorySupplier directorySupplier,
                                   IvHolder ivHolder) {
    this(tlogFile,
          globalStrings,
          openExisting,
          new EncryptionOutputStreamOpener(directorySupplier, ivHolder),
          new EncryptionChannelInputStreamOpener(directorySupplier, ivHolder));
  }

  protected EncryptionTransactionLog(Path tlogFile,
                                     Collection<String> globalStrings,
                                     boolean openExisting,
                                     EncryptionOutputStreamOpener outputStreamOpener,
                                     EncryptionChannelInputStreamOpener channelInputStreamOpener) {
    super(tlogFile,
          globalStrings,
          openExisting,
          outputStreamOpener,
          channelInputStreamOpener);
  }

  /**
   * Makes this transaction log readonly.
   * This method must only be called if {@link #refCount()} is 1.
   */
  public void readOnly() throws IOException {
    if (refCount() > 1) {
      throw new IllegalStateException("Cannot make a transaction log readonly while it is still in use");
    }
    // From that point, we don't expect any writes to reach the underlying OutputStream.
    // Flush is ok if it actually does not write anything.
    os.close();
  }

  public Path path() {
    return tlog;
  }

  public int refCount() {
    return refcount.get();
  }

  @Override
  protected void setWrittenCount(long fileStartOffset) throws IOException {
    if (os instanceof EncryptingOutputStream) {
      // Subtract the encryption header from the start offset.
      fos.setWritten(fileStartOffset - ENCRYPTION_FULL_HEADER_LENGTH);
      assert fos.size() == getLogFileSize();
    } else {
      super.setWrittenCount(fileStartOffset);
    }
  }

  @Override
  protected long getLogFileSize() throws IOException {
    long channelSize = super.getLogFileSize();
    if (os instanceof EncryptingOutputStream) {
      // Subtract the encryption header from the log file size to provide the actual data size.
      channelSize -= ENCRYPTION_FULL_HEADER_LENGTH;
    }
    return channelSize;
  }

  /** Writes the encryption magic header and the key reference number. */
  static void writeEncryptionHeader(String keyRef, OutputStream outputStream) throws IOException {
    writeBEInt(outputStream, ENCRYPTION_MAGIC);
    writeBEInt(outputStream, Integer.parseInt(keyRef));
  }

  private static void writeBEInt(OutputStream outputStream, int i) throws IOException {
    outputStream.write(i >> 24);
    outputStream.write(i >> 16);
    outputStream.write(i >> 8);
    outputStream.write(i);
  }

  /**
   * Reads the encryption magic header and the key reference number.
   *
   * @return The key reference number as a string; or null if the log is not encrypted.
   */
  static String readEncryptionHeader(FileChannel channel) throws IOException {
    int magic = readBEInt(channel, 0L, false);
    String keyRef = null;
    if (magic == ENCRYPTION_MAGIC) {
      // This file is encrypted.
      // Read the key reference that follows.
      keyRef = Integer.toString(readBEInt(channel, 4L, true));
    }
    return keyRef;
  }

  private static int readBEInt(FileChannel channel, long position, boolean requireAllBytes) throws IOException {
    ByteBuffer readBuffer = ByteBuffer.allocate(4);
    // Read 4 bytes.
    int bytesRead = channel.read(readBuffer, position);
    if (bytesRead < 4) {
      if (requireAllBytes) {
        throw new EOFException(
            bytesRead == -1 || bytesRead == 0
                ? "Header is empty; no data read."
                : "Incomplete header; expected 4 bytes, but only read " + bytesRead + " bytes.");
      } else {
        // If not requiring all bytes, just return 0.
        return 0;
      }
    }
    // Convert the 4 bytes to an integer in big-endian order
    return ((readBuffer.get(0) & 0xFF) << 24)
      | ((readBuffer.get(1) & 0xFF) << 16)
      | ((readBuffer.get(2) & 0xFF) << 8)
      | (readBuffer.get(3) & 0xFF);
  }

  /** Supplies and releases {@link EncryptionDirectory}. */
  public interface EncryptionDirectorySupplier {

    EncryptionDirectory get();

    void release(EncryptionDirectory directory) throws IOException;
  }

  /** Holds the IV only during the constructor call. */
  protected static class IvHolder {
    private byte[] iv;
  }

  protected static class EncryptionOutputStreamOpener implements OutputStreamOpener {

    protected final EncryptionDirectorySupplier directorySupplier;
    protected final IvHolder ivHolder;

    protected EncryptionOutputStreamOpener(EncryptionDirectorySupplier directorySupplier, IvHolder ivHolder) {
      this.directorySupplier = directorySupplier;
      this.ivHolder = ivHolder;
    }

    @Override
    public OutputStream open(FileChannel channel, long position) throws IOException {
      OutputStream outputStream = OUTPUT_STREAM_OPENER.open(channel, position);
      if (position != 0 && ivHolder.iv == null) {
        return outputStream;
      }
      EncryptionDirectory directory = directorySupplier.get();
      try {
        String keyRef = getActiveKeyRefFromCommit(directory.getLatestCommitData().data);
        if (keyRef != null) {
          // Get the key secret first. If it fails, we do not write anything.
          byte[] keySecret = directory.getKeySecret(keyRef);
          // The output stream has to be wrapped to be encrypted with the key.
          writeEncryptionHeader(keyRef, outputStream);
          EncryptingOutputStream eos = createEncryptingOutputStream(outputStream,
                                                                    position,
                                                                    ivHolder.iv,
                                                                    keySecret,
                                                                    directory.getEncrypterFactory());
          ivHolder.iv = eos.getIv();
          return eos;
        }
        ivHolder.iv = null;
        return outputStream;
      } finally {
        directorySupplier.release(directory);
      }
    }

    protected EncryptingOutputStream createEncryptingOutputStream(OutputStream outputStream,
                                                                  long position,
                                                                  byte[] iv,
                                                                  byte[] key,
                                                                  AesCtrEncrypterFactory factory)
      throws IOException {
      return new EncryptingOutputStream(outputStream, position, iv, key, factory);
    }
  }

  protected static class EncryptionChannelInputStreamOpener implements ChannelInputStreamOpener {

    protected final EncryptionDirectorySupplier directorySupplier;
    protected final IvHolder ivHolder;

    protected EncryptionChannelInputStreamOpener(
      EncryptionDirectorySupplier directorySupplier, IvHolder ivHolder) {
      this.directorySupplier = directorySupplier;
      this.ivHolder = ivHolder;
    }

    @Override
    public ChannelFastInputStream open(FileChannel channel, long position) throws IOException {
      EncryptionDirectory directory = directorySupplier.get();
      try {
        String keyRef = readEncryptionHeader(channel);
        if (keyRef != null) {
          // The IndexInput has to be wrapped to be decrypted with the key.
          DecryptingChannelInputStream dcis =
            createDecryptingChannelInputStream(
              channel,
              ENCRYPTION_KEY_HEADER_LENGTH,
              position,
              directory.getKeySecret(keyRef),
              directory.getEncrypterFactory());
          ivHolder.iv = dcis.getIv();
          return dcis;
        }
      } finally {
        directorySupplier.release(directory);
      }
      ivHolder.iv = null;
      return CHANNEL_INPUT_STREAM_OPENER.open(channel, position);
    }

    protected DecryptingChannelInputStream createDecryptingChannelInputStream(FileChannel channel,
                                                                              long offset,
                                                                              long position,
                                                                              byte[] key,
                                                                              AesCtrEncrypterFactory factory)
      throws IOException {
      return new DecryptingChannelInputStream(channel, offset, position, key, factory);
    }
  }
}
