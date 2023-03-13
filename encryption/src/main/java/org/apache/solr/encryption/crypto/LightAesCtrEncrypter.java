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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.lucene.util.SuppressForbidden;

import static org.apache.solr.encryption.crypto.AesCtrUtil.*;

/**
 * Hack {@link AesCtrEncrypter} equivalent to {@link javax.crypto.Cipher} with "AES/CTR/NoPadding" but more efficient.
 * <p>The {@link #LightAesCtrEncrypter(byte[], byte[]) constructor} and the {@link #init(long)} operations are lighter and
 * faster; {@link #clone()} is much faster. But it needs to call internal private {@code com.sun.crypto.provider.CounterMode}
 * with reflection. It may not be {@link #isSupported() supported}.
 * <p>Why do we need to access private {@code com.sun.crypto.provider.CounterMode} and {@code com.sun.crypto.provider.AESCrypt}?
 * Because they contain the special JVM annotation @HotSpotIntrinsicCandidate that makes their encryption method extremely
 * fast. If we copy the code in pure Java, it runs 30x slower.
 */
public class LightAesCtrEncrypter implements AesCtrEncrypter {

  /**
   * {@link LightAesCtrEncrypter} factory.
   */
  public static final AesCtrEncrypterFactory FACTORY = new Factory();

  private static final Constructor<?> AES_CRYPT_CONSTRUCTOR;
  private static final Method AES_CRYPT_INIT_METHOD;
  private static final Constructor<?> COUNTER_MODE_CONSTRUCTOR;
  private static final Field COUNTER_MODE_IV_FIELD;
  private static final Method COUNTER_MODE_RESET_METHOD;
  private static final Method COUNTER_MODE_CRYPT_METHOD;
  private static final Throwable HACK_FAILURE;
  static {
    Hack hack = AccessController.doPrivileged((PrivilegedAction<Hack>) LightAesCtrEncrypter::hack);
    AES_CRYPT_CONSTRUCTOR = hack.aesCryptConstructor;
    AES_CRYPT_INIT_METHOD = hack.aesCryptInitMethod;
    COUNTER_MODE_CONSTRUCTOR = hack.counterModeConstructor;
    COUNTER_MODE_IV_FIELD = hack.counterIvField;
    COUNTER_MODE_RESET_METHOD = hack.counterModeResetMethod;
    COUNTER_MODE_CRYPT_METHOD = hack.counterModeCryptMethod;
    HACK_FAILURE = hack.hackFailure;
  }

  private final Object aesCrypt;
  private final byte[] initialIv;
  private Object counterMode;
  private byte[] iv;

  /**
   * Indicates whether the {@link LightAesCtrEncrypter} hack is supported.
   * If it is not supported, then {@link LightAesCtrEncrypter} constructor throws an {@link UnsupportedOperationException}
   * (with the hack failure cause).
   */
  public static boolean isSupported() {
    return HACK_FAILURE == null;
  }

  /**
   * @param key The encryption key. It is cloned internally, its content is not modified, and no reference to it is kept.
   * @param iv  The Initialization Vector (IV) for the CTR mode. It MUST be random for the effectiveness of the encryption.
   *            It can be public (for example stored clear at the beginning of the encrypted file). It is cloned internally,
   *            its content is not modified, and no reference to it is kept.
   * @throws UnsupportedOperationException If the hack is not {@link #isSupported() supported}.
   */
  public LightAesCtrEncrypter(byte[] key, byte[] iv) {
    if (HACK_FAILURE != null) {
      throw new UnsupportedOperationException(HACK_FAILURE);
    }
    checkAesKey(key);
    try {
      aesCrypt = AES_CRYPT_CONSTRUCTOR.newInstance();
      AES_CRYPT_INIT_METHOD.invoke(aesCrypt, false, "AES", key);
      counterMode = COUNTER_MODE_CONSTRUCTOR.newInstance(aesCrypt);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.initialIv = iv.clone();
    this.iv = iv.clone();
  }

  @Override
  public void init(long counter) {
    checkCtrCounter(counter);
    buildAesCtrIv(initialIv, counter, iv);
    try {
      COUNTER_MODE_IV_FIELD.set(counterMode, iv);
      COUNTER_MODE_RESET_METHOD.invoke(counterMode);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(ByteBuffer inBuffer, ByteBuffer outBuffer) {
    assert inBuffer.array() != outBuffer.array() : "Input and output buffers must not be backed by the same array";
    int length = inBuffer.remaining();
    if (length > outBuffer.remaining()) {
      throw new IllegalArgumentException("Output buffer does not have enough remaining space (needs " + length + " B)");
    }
    int outPos = outBuffer.position();
    try {
      COUNTER_MODE_CRYPT_METHOD.invoke(counterMode, inBuffer.array(), inBuffer.arrayOffset() + inBuffer.position(),
        length, outBuffer.array(), outBuffer.arrayOffset() + outPos);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    inBuffer.position(inBuffer.limit());
    outBuffer.position(outPos + length);
  }

  @Override
  public LightAesCtrEncrypter clone() {
    LightAesCtrEncrypter clone;
    try {
      clone = (LightAesCtrEncrypter) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new Error("Failed to clone " + LightAesCtrEncrypter.class.getSimpleName() + "; this should not happen");
    }
    // aesCrypt and initialIv are the same references.
    try {
      clone.counterMode = COUNTER_MODE_CONSTRUCTOR.newInstance(aesCrypt);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    clone.iv = initialIv.clone();
    return clone;
  }

  @SuppressForbidden(reason = "Needs access to private APIs in com.sun.crypto.provider.CounterMode and com.sun.crypto.provider.AESCrypt to enable the hack")
  private static Hack hack() {
    Hack hack = new Hack();
    try {
      Class<?> aesCryptClass = Class.forName("com.sun.crypto.provider.AESCrypt");
      hack.aesCryptConstructor = aesCryptClass.getDeclaredConstructor();
      hack.aesCryptConstructor.setAccessible(true);
      hack.aesCryptInitMethod = aesCryptClass.getDeclaredMethod("init", boolean.class, String.class, byte[].class);
      hack.aesCryptInitMethod.setAccessible(true);
      Class<?> counterModeClass = Class.forName("com.sun.crypto.provider.CounterMode");
      Class<?> symmetricCipherClass = Class.forName("com.sun.crypto.provider.SymmetricCipher");
      hack.counterModeConstructor = counterModeClass.getDeclaredConstructor(symmetricCipherClass);
      hack.counterModeConstructor.setAccessible(true);
      Class<?> feedbackCipherClass = Class.forName("com.sun.crypto.provider.FeedbackCipher");
      hack.counterIvField = feedbackCipherClass.getDeclaredField("iv");
      hack.counterIvField.setAccessible(true);
      hack.counterModeResetMethod = counterModeClass.getDeclaredMethod("reset");
      hack.counterModeResetMethod.setAccessible(true);
      hack.counterModeCryptMethod = counterModeClass.getDeclaredMethod("implCrypt", byte[].class, int.class, int.class, byte[].class, int.class);
      hack.counterModeCryptMethod.setAccessible(true);
    } catch (SecurityException se) {
      hack.hackFailure = new UnsupportedOperationException(LightAesCtrEncrypter.class.getName() + " is not supported"
        + " because not all required permissions are given to the Lucene JAR file: " + se +
        " [To support it, grant at least the following permissions:" +
        " RuntimePermission(\"accessClassInPackage.com.sun.crypto.provider\") " +
        " and ReflectPermission(\"suppressAccessChecks\")]", se);
    } catch (ReflectiveOperationException | RuntimeException e) {
      hack.hackFailure = new UnsupportedOperationException(LightAesCtrEncrypter.class.getName() + " is not supported"
        + " on this platform because internal Java APIs are not compatible with this Lucene version: " + e, e);
    }
    if (hack.hackFailure != null) {
      hack.aesCryptConstructor = null;
      hack.aesCryptInitMethod = null;
      hack.counterModeConstructor = null;
      hack.counterIvField = null;
      hack.counterModeResetMethod = null;
      hack.counterModeCryptMethod = null;
    }
    return hack;
  }

  private static class Hack {
    Constructor<?> aesCryptConstructor;
    Method aesCryptInitMethod;
    Constructor<?> counterModeConstructor;
    Field counterIvField;
    Method counterModeResetMethod;
    Method counterModeCryptMethod;
    UnsupportedOperationException hackFailure;
  }

  /**
   * {@link LightAesCtrEncrypter} factory.
   */
  public static class Factory implements AesCtrEncrypterFactory {
    @Override
    public AesCtrEncrypter create(byte[] key, byte[] iv) {
      return new LightAesCtrEncrypter(key, iv);
    }

    @Override
    public boolean isSupported() {
      return LightAesCtrEncrypter.isSupported();
    }

    @Override
    public Throwable getUnsupportedCause() {
      return LightAesCtrEncrypter.HACK_FAILURE;
    }
  }
}