package org.apache.solr.encryption.kms;

import org.apache.solr.common.util.NamedList;

import static org.apache.solr.encryption.EncryptionTestUtil.TENANT_ID;
import static org.apache.solr.encryption.EncryptionTestUtil.generateMockKeyBlob;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_SECRETS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Mock {@link KmsClient} implementation for tests.
 * <p>
 * Extending classes could override {@link #decrypt} and {@link #generateKeyBlob} to run tests
 * with real encryption keys.
 */
public class TestingKmsClient implements KmsClient {

    public static volatile TestingKmsClient singleton;

    protected TestingKmsClient() {
        singleton = this;
    }

    @Override
    public byte[] decrypt(String keyId, String keyBlob, String tenantId, String requestId) {
        byte[] secret = KEY_SECRETS.get(keyId);
        assertNotNull("No key defined for " + keyId, secret);
        // See EncryptionTestUtil.KEY_BLOB structure.
        assertTrue("Invalid key blob", keyBlob.startsWith("{\"keyId\":\"" + keyId + "\""));
        assertEquals(TENANT_ID, tenantId);
        assertNotNull(requestId);
        assertNotEquals("", requestId);
        return secret;
    }

    /**
     * Generates the key blob.
     */
    public String generateKeyBlob(String keyId, String tenantId) throws Exception {
        return generateMockKeyBlob(keyId);
    }

    @Override
    public void close() {
    }

    /**
     * Creates a {@link TestingKmsClient}.
     */
    public static class Factory implements KmsClient.Factory {

        @Override
        public KmsClient create(NamedList<?> args) {
            return new TestingKmsClient();
        }
    }
}
