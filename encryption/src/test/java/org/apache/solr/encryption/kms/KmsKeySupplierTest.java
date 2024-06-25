package org.apache.solr.encryption.kms;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.encryption.EncryptionTestUtil;
import org.apache.solr.encryption.KeySupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_ENCRYPTION_KEY_BLOB;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_TENANT_ID;
import static org.apache.solr.encryption.kms.KmsKeySupplier.Factory.PARAM_KMS_CLIENT_FACTORY;
import static org.apache.solr.encryption.matcher.EventuallyMatcher.eventually;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Tests {@link KmsKeySupplier} and its {@link KmsKeyCache}.
 */
public class KmsKeySupplierTest extends SolrTestCaseJ4 {

    private static final Map<String, String> SECRETS = Map.of(
            "k1", "s1",
            "k2", "s2"
    );
    private static final String BLOB_SUFFIX = "Blob";

    private SpyingKeySupplierFactory keySupplierFactory;
    private MockKmsClient kmsClient;
    private CookieSupplier cookieSupplier;
    private KeySupplier keySupplier;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        initCore("solrconfig.xml", "schema.xml", EncryptionTestUtil.getConfigPath().toString());
        keySupplierFactory = new SpyingKeySupplierFactory();
        NamedList<String> args = new NamedList<>();
        args.add(PARAM_KMS_CLIENT_FACTORY, TestingKmsClient.Factory.class.getName());
        keySupplierFactory.init(args, h.getCoreContainer());
        keySupplierFactory.kmsMetrics.reset();
        kmsClient = keySupplierFactory.kmsClient;
        cookieSupplier = new CookieSupplier();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (keySupplier != null) {
                keySupplier.close();
            }
            deleteCore();
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testKeyCacheMissAndHit() throws Exception {
        keySupplier = keySupplierFactory.create();

        verifyCacheMiss("k1");
        verifyCacheHit("k1");
        verifyCacheMiss("k2");
        verifyCacheHit("k2");
        verifyCacheHit("k1");

        assertKeyDecryptMetrics(2, 0);
    }

    @Test
    public void testKeyCacheExpiration() throws Exception {
        Duration cacheExpiration = Duration.ofMillis(300);
        AtomicLong timeNs = new AtomicLong(System.nanoTime());
        keySupplierFactory.setCacheExpiration(cacheExpiration);
        keySupplierFactory.setTicker(timeNs::get);
        keySupplier = keySupplierFactory.create();

        verifyCacheMiss("k1");
        verifyCacheHit("k1");
        assertEquals(List.of(), keySupplierFactory.removedKeyIds);
        // Verify that without interaction, the entry is removed after the expiration.
        // We use the test ticker to simulate the time elapsed.
        timeNs.addAndGet(cacheExpiration.getNano() * 2L);
        // Wait a bit for the Caffeine cache to read the ticker. Caffeine batches checks
        // and operations each second; it is not configurable.
        assertThat(() -> keySupplierFactory.removedKeyIds, eventually(equalTo(List.of("k1")), Duration.ofSeconds(2)));

        keySupplierFactory.removedKeyIds.clear();
        verifyCacheMiss("k1");
        verifyCacheHit("k1");
        assertEquals(List.of(), keySupplierFactory.removedKeyIds);

        assertKeyDecryptMetrics(2, 0);
    }

    @Test
    public void testExceptionHandling() throws Exception {
        AtomicLong timeNs = new AtomicLong(System.nanoTime());
        keySupplierFactory.setTicker(timeNs::get);
        keySupplier = keySupplierFactory.create();

        // Verify that an unknown key exception is propagated.
        try {
            keySupplier.getKeySecret("k3", cookieSupplier);
            fail("Expected exception not thrown");
        } catch (NoSuchElementException e) {
            // Expected.
        }

        // Verify that any KMS exception is propagated.
        try {
            kmsClient.simulatedException = new Exception("Test");
            keySupplier.getKeySecret("k1", cookieSupplier);
            fail("Expected exception not thrown");
        } catch (RuntimeException e) {
            // Expected.
            assertEquals(kmsClient.simulatedException, e.getCause());
            kmsClient.simulatedException = null;
        }

        // Verify that the next same call also fails with a special delay exception.
        try {
            keySupplier.getKeySecret("k1", cookieSupplier);
            fail("Expected exception not thrown");
        } catch (SolrException e) {
            // Expected.
            assertEquals(SolrException.ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
        }

        // Verify that any IO exception is propagated.
        timeNs.addAndGet(KmsKeySupplier.FAILED_DECRYPT_DELAY_NS);
        try {
            kmsClient.simulatedException = new IOException("Test");
            keySupplier.getKeySecret("k1", cookieSupplier);
            fail("Expected exception not thrown");
        } catch (RuntimeException e) {
            // Expected.
            assertEquals(kmsClient.simulatedException, e.getCause());
            kmsClient.simulatedException = null;
        }

        assertKeyDecryptMetrics(3,3);
    }

    @Test
    public void testDelayAfterFailedDecrypt() throws Exception {
        AtomicLong timeNs = new AtomicLong(System.nanoTime());
        keySupplierFactory.setTicker(timeNs::get);
        keySupplier = keySupplierFactory.create();

        // When a call to KMS decrypt fails with an exception.
        try {
            kmsClient.simulatedException = new Exception("Test");
            keySupplier.getKeySecret("k1", cookieSupplier);
            fail("Expected exception not thrown");
        } catch (RuntimeException e) {
            // Expected.
            assertEquals(kmsClient.simulatedException, e.getCause());
            kmsClient.simulatedException = null;
        }

        // Then the next same call also fails with a special delay exception.
        try {
            keySupplier.getKeySecret("k1", cookieSupplier);
            fail("Expected exception not thrown");
        } catch (SolrException e) {
            // Expected.
            assertEquals(SolrException.ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
        }

        // And when the delay expires.
        timeNs.addAndGet(KmsKeySupplier.FAILED_DECRYPT_DELAY_NS);

        // Then the same call succeeds.
        verifyCacheMiss("k1");
        verifyCacheHit("k1");

        assertKeyDecryptMetrics(2,1);
    }

    private void verifyCacheMiss(String key) throws Exception {
        cookieSupplier.processedKeyIds.clear();
        kmsClient.processedKeyIds.clear();
        byte[] keySecret = keySupplier.getKeySecret(key, cookieSupplier);
        assertEquals(SECRETS.get(key), secret(keySecret));
        assertEquals(List.of(key), cookieSupplier.processedKeyIds);
        assertEquals(List.of(key), kmsClient.processedKeyIds);
    }

    private void verifyCacheHit(String key) throws Exception {
        cookieSupplier.processedKeyIds.clear();
        kmsClient.processedKeyIds.clear();
        byte[] keySecret = keySupplier.getKeySecret(key, cookieSupplier);
        assertEquals(SECRETS.get(key), secret(keySecret));
        assertEquals(List.of(), cookieSupplier.processedKeyIds);
        assertEquals(List.of(), kmsClient.processedKeyIds);
    }

    private static String secret(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void assertKeyDecryptMetrics(int expectedNumKeyDecrypt, int expectedNumFailedKeyDecrypt) {
        assertEquals(expectedNumKeyDecrypt, keySupplierFactory.kmsMetrics.getNumKeyDecrypt());
        assertEquals(expectedNumFailedKeyDecrypt, keySupplierFactory.kmsMetrics.getNumFailedKeyDecrypt());
    }

    private static class MockKmsClient implements KmsClient {

        List<String> processedKeyIds = Collections.synchronizedList(new ArrayList<>());
        volatile Exception simulatedException;
        boolean closeCalled;

        @Override
        public byte[] decrypt(String keyId, String dataKeyBlob, String tenantId, String requestId)
                throws Exception {
            if (simulatedException != null) {
                throw simulatedException;
            }
            assertTrue(dataKeyBlob.endsWith(BLOB_SUFFIX));
            String expectedKeyId = dataKeyBlob.substring(0, dataKeyBlob.length() - BLOB_SUFFIX.length());
            assertEquals(expectedKeyId, keyId);
            processedKeyIds.add(keyId);
            String secret = SECRETS.get(keyId);
            if (secret == null) {
                throw new NoSuchElementException();
            }
            return secret.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    private static class CookieSupplier implements Function<String, Map<String, String>> {

        List<String> processedKeyIds = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Map<String, String> apply(String keyId) {
            processedKeyIds.add(keyId);
            return Map.of(PARAM_ENCRYPTION_KEY_BLOB, keyId + BLOB_SUFFIX, PARAM_TENANT_ID, "tenantId");
        }
    }

    private static class SpyingKeySupplierFactory extends KmsKeySupplier.Factory {

        CoreContainer coreContainer;
        MockKmsClient kmsClient = new MockKmsClient();
        Duration cacheExpiration;
        Ticker ticker = Ticker.systemTicker();
        SpyingKmsMetrics kmsMetrics;
        List<String> removedKeyIds = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void init(NamedList<?> args, CoreContainer coreContainer) {
            coreContainer.getObjectCache().remove(KEY_SUPPLIER);
            super.init(args, coreContainer);
            this.coreContainer = coreContainer;
        }

        void setCacheExpiration(Duration cacheExpiration) {
            this.cacheExpiration = cacheExpiration;
        }

        void setTicker(Ticker ticker) throws IOException {
            this.ticker = ticker;
            KeySupplier keySupplier = (KeySupplier) coreContainer.getObjectCache().remove(KEY_SUPPLIER);
            keySupplier.close();
            NamedList<String> args = new NamedList<>();
            args.add(PARAM_KMS_CLIENT_FACTORY, TestingKmsClient.Factory.class.getName());
            init(args, coreContainer);
        }

        @Override
        protected Duration getKeyCacheExpiration(NamedList<?> args) {
            if (cacheExpiration != null) {
                return cacheExpiration;
            }
            return super.getKeyCacheExpiration(args);
        }

        @Override
        protected KmsKeyCache createKeyCache(Duration cacheExpiration) {
            return new SpyingKeyCache(cacheExpiration, ticker);
        }

        @Override
        protected KeySupplier createKeySupplier(
                KmsClient kmsClient,
                KmsKeyCache kmsKeyCache,
                KmsMetrics kmsMetrics) {
            return new KmsKeySupplier(
                    this.kmsClient,
                    kmsKeyCache,
                    new ConcurrentHashMap<>(),
                    kmsMetrics,
                    ticker);
        }

        @Override
        protected KmsMetrics createEncryptionMetrics(CoreContainer coreContainer) {
            return kmsMetrics = new SpyingKmsMetrics(coreContainer);
        }

        class SpyingKeyCache extends KmsKeyCache {

            SpyingKeyCache(Duration cacheExpiration, Ticker ticker) {
                super(cacheExpiration, ticker);
            }

            @Override
            protected RemovalListener<String, byte[]> createCacheRemovalListener() {
                RemovalListener<String, byte[]> delegate = super.createCacheRemovalListener();
                return (keyId, secret, removalCause) -> {
                    removedKeyIds.add(keyId);
                    delegate.onRemoval(keyId, secret, removalCause);
                };
            }
        }
    }
}
