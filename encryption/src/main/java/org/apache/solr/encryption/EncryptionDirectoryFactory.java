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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.MMapDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.apache.solr.encryption.crypto.CipherAesCtrEncrypter;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Creates an {@link EncryptionDirectory} delegating to a {@link org.apache.lucene.store.MMapDirectory}.
 * <p/>
 * To be configured with two parameters:
 * <ul>
 *   <li>{@link #PARAM_KEY_SUPPLIER_FACTORY} defines the {@link KeySupplier.Factory} to use.
 *   Required.</li>
 *   <li>{@link #PARAM_ENCRYPTER_FACTORY} defines which {@link AesCtrEncrypterFactory} to use.
 *   Optional; default is {@link CipherAesCtrEncrypter.Factory}.</li>
 * </ul>
 * <pre>
 *   <directoryFactory name="DirectoryFactory"
 *       class="${solr.directoryFactory:org.apache.solr.encryption.EncryptionDirectoryFactory}">
 *     <str name="keySupplierFactory">${solr.keySupplierFactory:com.myproject.MyKeySupplierFactory}</str>
 *     <str name="encrypterFactory">${solr.encrypterFactory:org.apache.solr.encryption.crypto.LightAesCtrEncrypter$Factory}</str>
 *   </directoryFactory>
 * </pre>
 */
public class EncryptionDirectoryFactory extends MMapDirectoryFactory {

  // TODO: Ideally EncryptionDirectoryFactory would extend a DelegatingDirectoryFactory to delegate
  //  to any other DirectoryFactory. There is a waiting Jira issue SOLR-15060 for that because
  //  a DelegatingDirectoryFactory is not straightforward. There is the tricky case of the
  //  CachingDirectoryFactory (extended by most DirectoryFactory implementations) that currently
  //  creates the Directory itself, so it would not be our delegating Directory.
  //  Right now, EncryptionDirectoryFactory extends MMapDirectoryFactory. And we hope we will
  //  refactor later.

  /**
   * Required Solr config parameter to define the {@link KeySupplier.Factory} class used
   * to create the {@link KeySupplier}.
   */
  public static final String PARAM_KEY_SUPPLIER_FACTORY = "keySupplierFactory";
  /**
   * Optional Solr config parameter to set the {@link AesCtrEncrypterFactory} class used
   * to create the {@link org.apache.solr.encryption.crypto.AesCtrEncrypter}.
   * The default is {@link CipherAesCtrEncrypter}.
   */
  public static final String PARAM_ENCRYPTER_FACTORY = "encrypterFactory";
  /**
   * Visible for tests only - Property defining the class name of the inner encryption directory factory.
   */
  static final String PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY = "innerEncryptionDirectoryFactory";

  private KeySupplier keySupplier;
  private AesCtrEncrypterFactory encrypterFactory;
  private InnerFactory innerFactory;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);

    String keySupplierFactoryClass = args._getStr(PARAM_KEY_SUPPLIER_FACTORY, System.getProperty("solr." + PARAM_KEY_SUPPLIER_FACTORY));
    if (keySupplierFactoryClass == null) {
      throw new IllegalArgumentException("Missing " + PARAM_KEY_SUPPLIER_FACTORY + " argument for " + getClass().getName());
    }
    KeySupplier.Factory keySupplierFactory = coreContainer.getResourceLoader().newInstance(keySupplierFactoryClass,
                                                                                          KeySupplier.Factory.class);
    keySupplierFactory.init(args, coreContainer);
    try {
      keySupplier = keySupplierFactory.create();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    String encrypterFactoryClass = args._getStr(PARAM_ENCRYPTER_FACTORY, CipherAesCtrEncrypter.Factory.class.getName());
    encrypterFactory = coreContainer.getResourceLoader().newInstance(encrypterFactoryClass,
                                                                     AesCtrEncrypterFactory.class);
    if (!encrypterFactory.isSupported()) {
      throw new UnsupportedOperationException(getClass().getName() + " cannot create an encrypterFactory of type "
                                                + encrypterFactory.getClass().getName(),
                                              encrypterFactory.getUnsupportedCause());
    }

    innerFactory = createInnerFactory();
  }

  private InnerFactory createInnerFactory() {
    String factoryClassName = System.getProperty(PROPERTY_INNER_ENCRYPTION_DIRECTORY_FACTORY);
    if (factoryClassName == null) {
      return EncryptionDirectory::new;
    }
    try {
      return (InnerFactory) EncryptionDirectoryFactory.class.getClassLoader()
        .loadClass(factoryClassName).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Cannot load custom inner directory factory " + factoryClassName, e);
    }
  }

  /** Gets the {@link AesCtrEncrypterFactory} used by this factory and all the encryption directories it creates. */
  public AesCtrEncrypterFactory getEncrypterFactory() {
    return encrypterFactory;
  }

  /** Gets the {@link KeySupplier} used by this factory and all the encryption directories it creates. */
  public KeySupplier getKeySupplier() {
    return keySupplier;
  }

  public static EncryptionDirectoryFactory getFactory(SolrCore core) {
    if (!(core.getDirectoryFactory() instanceof EncryptionDirectoryFactory)) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                              DirectoryFactory.class.getSimpleName()
                                + " must be configured with an "
                                + EncryptionDirectoryFactory.class.getSimpleName());
    }
    return (EncryptionDirectoryFactory) core.getDirectoryFactory();
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    return innerFactory.create(super.create(path, lockFactory, dirContext), getEncrypterFactory(), getKeySupplier());
  }

  @Override
  public void close() throws IOException {
    keySupplier.close();
    super.close();
  }

  /**
   * Visible for tests only - Inner factory that creates {@link EncryptionDirectory} instances.
   */
  interface InnerFactory {
    EncryptionDirectory create(Directory delegate, AesCtrEncrypterFactory encrypterFactory, KeySupplier keySupplier)
      throws IOException;
  }
}
