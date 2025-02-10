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

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.io.output.AppendableWriter;
import org.apache.commons.io.output.WriterOutputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Encrypts a character stream to a base 64 encoding compatible with JSON.
 * <p>
 * The whole encryption and base 64 encoding process is streamed, with no large
 * buffers allocated. The encryption transformation is AES/CTR/NoPadding.
 * A secure random IV is generated for each encryption and appended as the first
 * appended chars.
 * <p>
 * This encryption tool is intended to encrypt write-once and then read-only strings,
 * it should not be used to encrypt updatable content as CTR is not designed for that.
 */
public class CharStreamEncrypter {

  private static final int BUFFER_MIN_SIZE = 128;
  private static final int BUFFER_MAX_SIZE = 8192;

  private final AesCtrEncrypterFactory factory;

  public CharStreamEncrypter(AesCtrEncrypterFactory factory) {
    this.factory = factory;
  }

  /**
   * Encrypts an input string to base 64 characters compatible with JSON.
   *
   * @param key AES key, can either 16, 24 or 32 bytes.
   * @return the encrypted base 64 chars.
   */
  public String encrypt(String input, byte[] key) {
    StringBuilder output = new StringBuilder(input.length() * 2);
    try {
      encrypt(input, key, output);
    } catch (IOException e) {
      // Never happens when appending to a StringBuilder.
      throw new UncheckedIOException(e);
    }
    return output.toString();
  }

  /**
   * Encrypts an input string to base 64 characters compatible with JSON.
   *
   * @param key    AES key, can either 16, 24 or 32 bytes.
   * @param output where to append the encrypted base 64 chars.
   * @throws IOException propagates any exception thrown when appending to the output.
   */
  public void encrypt(String input, byte[] key, Appendable output)
    throws IOException {
    encrypt(new StringReader(input), input.length(), key, output);
  }

  /**
   * Encrypts a char reader stream to base 64 characters compatible with JSON.
   *
   * @param inputSizeHint optional hint for the input size; or -1 if unknown.
   * @param key           AES key, can either 16, 24 or 32 bytes.
   * @param output        where to append the encrypted base 64 chars.
   * @throws IOException propagates any exception thrown when appending to the output.
   */
  public void encrypt(Reader inputReader, int inputSizeHint, byte[] key, Appendable output)
    throws IOException {
    // Don't use jdk Base64.getEncoder().wrap() because it's buggy.
    int bufferSize = getBufferSize(inputSizeHint);
    try (OutputStreamWriter encryptedOutputWriter =
           new OutputStreamWriter(
             new EncryptingOutputStream(
               new Base64OutputStream(
                 new LightWriterOutputStream(
                   toWriter(output),
                   StandardCharsets.ISO_8859_1,
                   bufferSize
                 )
               ),
               key,
               factory
             ),
             StandardCharsets.UTF_8
           )
    ) {
      transfer(inputReader,
               encryptedOutputWriter,
               bufferSize
      );
    }
  }

  /**
   * Decrypts an input string previously encrypted with {@link #encrypt}.
   *
   * @param key AES key, can either 16, 24 or 32 bytes.
   * @return the decrypted chars.
   */
  public String decrypt(String input, byte[] key) {
    StringBuilder output = new StringBuilder((int) (input.length() * 0.6f));
    try {
      decrypt(input, key, output);
    } catch (IOException e) {
      // Never happens when appending to a StringBuilder.
      throw new UncheckedIOException(e);
    }
    return output.toString();
  }

  /**
   * Decrypts an input string previously encrypted with {@link #encrypt}.
   *
   * @param key    AES key, can either 16, 24 or 32 bytes.
   * @param output where to append the decrypted chars.
   * @throws IOException propagates any exception thrown when appending to the output.
   */
  public void decrypt(String input, byte[] key, Appendable output)
    throws IOException {
    decrypt(new StringReader(input), input.length(), key, output);
  }

  /**
   * Decrypts a char reader stream previously encrypted with {@link #encrypt}.
   *
   * @param inputSizeHint optional hint for the input size; or -1 if unknown.
   * @param key           AES key, can either 16, 24 or 32 bytes.
   * @param output        where to append the decrypted chars.
   * @throws IOException propagates any exception thrown when appending to the output.
   */
  public void decrypt(Reader inputReader, int inputSizeHint, byte[] key, Appendable output)
    throws IOException {
    // Don't use jdk Base64.getDecoder().wrap() because it's buggy.
    int bufferSize = getBufferSize(inputSizeHint);
    try (InputStreamReader decryptedInputReader =
           new InputStreamReader(
             new DecryptingInputStream(
               new Base64InputStream(
                 new ReaderInputStream(
                   inputReader,
                   StandardCharsets.ISO_8859_1,
                   bufferSize
                 )
               ),
               key,
               factory
             ),
             StandardCharsets.UTF_8
           )
    ) {
      transfer(decryptedInputReader,
               toWriter(output),
               bufferSize
      );
    }
  }

  private static int getBufferSize(int inputSizeHint) {
    return inputSizeHint < 0 ? BUFFER_MAX_SIZE
      : Math.min(Math.max(inputSizeHint / 16, BUFFER_MIN_SIZE), BUFFER_MAX_SIZE);
  }

  private static Writer toWriter(Appendable appendable) {
    return appendable instanceof Writer ? (Writer) appendable : new AppendableWriter<>(appendable);
  }

  /**
   * Similar to {@link Reader#transferTo(Writer)} with a provided buffer size.
   */
  private static void transfer(Reader input, Writer output, int bufferSize)
    throws IOException {
    char[] buffer = new char[bufferSize];
    int nRead;
    while ((nRead = input.read(buffer, 0, bufferSize)) >= 0) {
      output.write(buffer, 0, nRead);
    }
  }

  /**
   * Same as {@link WriterOutputStream} without creating a buffer for each
   * call to {@link #write(int)}.
   */
  private static class LightWriterOutputStream extends WriterOutputStream {

    private final byte[] oneByteBuf = new byte[1];

    public LightWriterOutputStream(Writer writer,
                                   Charset charset,
                                   int bufferSize) {
      super(writer, charset, bufferSize, false);
    }

    @Override
    public void write(int b) throws IOException {
      oneByteBuf[0] = (byte) b;
      write(oneByteBuf, 0, 1);
    }
  }
}
