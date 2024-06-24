package org.apache.solr.encryption.kms;

import org.apache.solr.common.util.NamedList;

import static org.apache.solr.encryption.EncryptionTestUtil.TENANT_ID;
import static org.apache.solr.encryption.TestingKeySupplier.KEY_SECRETS;
import static org.apache.solr.encryption.kms.KmsKeySupplier.Factory.PARAM_KMS_CLIENT_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Mock {@link KmsClient} implementation for tests.
 */
public class TestingKmsClient implements KmsClient {

    @Override
    public byte[] decrypt(String keyId, String keyBlob, String tenantId, String requestId) {
        byte[] secret = KEY_SECRETS.get(keyId);
        assertNotNull("No key defined for " + keyId, secret);
        assertTrue("Invalid key blob", keyBlob.startsWith("keyId=\"" + keyId + "\""));
        assertEquals(TENANT_ID, tenantId);
        assertNotNull(requestId);
        assertNotEquals("", requestId);
        return secret;
    }

    @Override
    public void close() {
    }

    public static class Factory implements KmsClient.Factory {

        @Override
        public KmsClient create(NamedList<?> args) {
            assertEquals(TestingKmsClient.Factory.class.getName(), args.get(PARAM_KMS_CLIENT_FACTORY));
            return new TestingKmsClient();
        }
    }
}
