package org.apache.solr.encryption.kms;

import org.apache.solr.common.util.NamedList;

import java.io.Closeable;

/**
 * Client that calls the Key Management System.
 */
public interface KmsClient extends Closeable {

    /**
     * Decrypts the key blob (ciphered form of the key) to get the clear-text key secret bytes.
     * @param keyId The key id passed as parameter to the {@link KmsEncryptionRequestHandler}.
     * @param keyBlob The key blob passed as parameter to the {@link KmsEncryptionRequestHandler}.
     *               It contains the ciphered key to decrypt, and may contain other data.
     * @param tenantId The identifier of the tenant owning the key.
     * @param requestId A request id for tracing/log purposes.
     * @return The clear-text key secret bytes.
     */
    byte[] decrypt(String keyId, String keyBlob, String tenantId, String requestId) throws Exception;

    /**
     * Creates a {@link KmsClient}.
     * Only one {@link KmsClient} singleton is created.
     */
    interface Factory {

        /**
         * Creates a {@link KmsClient}.
         * @param args The Solr config parameters of the {@code directoryFactory} section.
         */
        KmsClient create(NamedList<?> args) throws Exception;
    }
}
