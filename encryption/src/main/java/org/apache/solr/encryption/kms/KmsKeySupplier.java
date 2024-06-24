package org.apache.solr.encryption.kms;

import com.github.benmanes.caffeine.cache.Ticker;

import io.opentracing.util.GlobalTracer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.encryption.KeySupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_ENCRYPTION_KEY_BLOB;
import static org.apache.solr.encryption.kms.KmsEncryptionRequestHandler.PARAM_TENANT_ID;

/**
 * Supplies keys by decrypting them with a Key Management System (KMS).
 * The keys are stored in their encrypted form in the index. The encrypted form is sent to KMS to decrypt it
 * and get the clear-text key secret. And the key secrets are cached in memory during a configurable short
 * duration, to avoid calling too often the KMS.
 * <p>
 * Thread safe.
 */
public class KmsKeySupplier implements KeySupplier {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Min delay between a failed call to KMS decrypt and the next attempt for the
     * same key, in ns.
     */
    public static final long FAILED_DECRYPT_DELAY_NS = TimeUnit.SECONDS.toNanos(10);

    /** Log when the KMS decrypt calls takes more time than this duration threshold, in ns. */
    private static final long KEY_DECRYPT_DURATION_THRESHOLD_NS = TimeUnit.SECONDS.toNanos(3);

    /**
     * File name extensions/suffixes that do NOT need to be encrypted because it lacks user/external data.
     * Other files should be encrypted.
     * There is some human judgement here as some files may contain vague clues as to the shape of the data.
     */
    private static final Set<String> CLEARTEXT_EXTENSIONS = Set.of(
            "doc",    // Document number, frequencies, and skip data
            "pos",    // Positions
            "pay",    // Payloads and offsets
            "dvm",    // Doc values metadata
            "fdm",    // Stored fields metadata
            "fdx",    // Stored fields index
            "nvd",    // Norms data
            "nvm",    // Norms metadata
            "vem",    // Vector metadata
            "fnm",    // Field Infos
            "si",     // Segment Infos
            "cfe"     // Compound file entries
    );
    // Extensions known to contain sensitive user data, and thus that need to be encrypted:
    // tip    - BlockTree terms index (FST)
    // tim    - BlockTree terms
    // tmd    - BlockTree metadata (contains first and last term)
    // fdt    - Stored fields data
    // dvd    - Doc values data
    // vex    - Vector index
    // cfs    - Compound file (contains all the above files data)

    // Cleartext temporary files:
    private static final String TMP_EXTENSION = "tmp";
    private static final String TMP_DOC_IDS = "-doc_ids"; // FieldsIndexWriter
    private static final String TMP_FILE_POINTERS = "file_pointers"; // FieldsIndexWriter

    private static final Base64.Encoder ID_ENCODER = Base64.getEncoder().withoutPadding();

    private final KmsClient kmsClient;
    private final KmsKeyCache kmsKeyCache;
    private final ConcurrentMap<String, Long> nextDecryptTimeCache;
    private final KmsMetrics kmsMetrics;
    private final Ticker ticker;

    protected KmsKeySupplier(
            KmsClient kmsClient,
            KmsKeyCache kmsKeyCache,
            ConcurrentMap<String, Long> nextDecryptTimeCache,
            KmsMetrics kmsMetrics,
            Ticker ticker) {
        this.kmsClient = kmsClient;
        this.kmsKeyCache = kmsKeyCache;
        this.nextDecryptTimeCache = nextDecryptTimeCache;
        this.kmsMetrics = kmsMetrics;
        this.ticker = ticker;
    }

    @Override
    public boolean shouldEncrypt(String fileName) {
        return shouldEncryptFile(fileName);
    }

    public static boolean shouldEncryptFile(String fileName) {
        String extension = IndexFileNames.getExtension(fileName);
        if (extension == null) {
            // segments and pending_segments are never passed as parameter of this method.
            assert !fileName.startsWith(IndexFileNames.SEGMENTS) && !fileName.startsWith(IndexFileNames.PENDING_SEGMENTS);
        } else if (CLEARTEXT_EXTENSIONS.contains(extension)) {
            // The file extension tells us it does not need to be encrypted.
            return false;
        } else if (extension.equals(TMP_EXTENSION)) {
            // We know some tmp files do not need to be encrypted.
            int tmpCounterIndex = fileName.lastIndexOf('_');
            assert tmpCounterIndex != -1;
            return !endsWith(fileName, TMP_DOC_IDS, tmpCounterIndex)
                    && !endsWith(fileName, TMP_FILE_POINTERS, tmpCounterIndex);
        }
        // By default, all other files should be encrypted.
        return true;
    }

    private static boolean endsWith(String s, String suffix, int endIndex) {
        // Inspired from JDK String where endsWith calls startsWith.
        // Here we look for [suffix] from index [endIndex - suffix.length()].
        // This is equivalent to
        // s.substring(0, endIndex).endsWith(suffix)
        // without creating a substring.
        return s.startsWith(suffix, endIndex - suffix.length());
    }

    @Nullable
    @Override
    public Map<String, String> getKeyCookie(String keyId, Map<String, String> params) throws IOException {
        // With this KeySupplier implementation, the key cookie (encrypted form) is expected to be passed
        // to the EncryptionRequestHandler.
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getKeySecret(String keyId, Function<String, Map<String, String>> cookieSupplier) throws IOException {
        // This method must be thread safe because there is only one instance of KeySupplier
        // per EncryptionDirectoryFactory.
        try {
            if (keyId == null) {
                // Short-circuit the key cache.
                return decryptOrDelay(null, cookieSupplier);
            }
            return kmsKeyCache.getCache().get(keyId, k -> decryptOrDelay(k, cookieSupplier));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private byte[] decryptOrDelay(String keyId, Function<String, Map<String, String>> cookieSupplier) {
        // Check if a call to decrypt the same key failed recently.
        String decryptTimeKeyId = keyId == null ?
                // If the key id is null, use this key blob string instead.
                // This way we can still use the anti-flood mechanism.
                cookieSupplier.apply(null).get(PARAM_ENCRYPTION_KEY_BLOB)
                : keyId;
        Long nextTimeNs = nextDecryptTimeCache.get(decryptTimeKeyId);
        if (nextTimeNs != null && ticker.read() < nextTimeNs) {
            // The key decryption failed very recently.
            // Do not flood-call KMS, fail with a delay exception.
            throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                    "Delaying the next attempt to decrypt key " + decryptTimeKeyId);
        }
        try {
            // Call KMS to decrypt the key.
            byte[] keySecret = decrypt(keyId, cookieSupplier);
            if (nextTimeNs != null) {
                nextDecryptTimeCache.remove(decryptTimeKeyId);
            }
            return keySecret;
        } catch (RuntimeException e) {
            // The call to KMS failed for some reason (network, unavailability, certificate, key id).
            // Record the next time to attempt the same call.
            nextDecryptTimeCache.put(decryptTimeKeyId, ticker.read() + FAILED_DECRYPT_DELAY_NS);
            throw e;
        }
    }

    private byte[] decrypt(String keyId, Function<String, Map<String, String>> cookieSupplier) {
        boolean success = false;
        try {
            kmsMetrics.incKeyDecrypt();
            Map<String, String> keyCookie = cookieSupplier.apply(keyId);
            long startTimeNs = ticker.read();
            byte[] keySecret = kmsClient.decrypt(
                    keyId,
                    keyCookie.get(PARAM_ENCRYPTION_KEY_BLOB),
                    keyCookie.get(PARAM_TENANT_ID),
                    getRequestId());
            long timeEndNs;
            if ((timeEndNs = ticker.read()) - startTimeNs >= KEY_DECRYPT_DURATION_THRESHOLD_NS) {
                kmsMetrics.incSlowKeyDecrypt();
                log.warn("decrypt took {} ms", TimeUnit.NANOSECONDS.toMillis(timeEndNs - startTimeNs));
            }
            success = true;
            return keySecret;
        } catch (RuntimeException e) {
            log.error("failed to decrypt DEK", e);
            throw e;
        } catch (Exception e) {
            log.error("failed to decrypt DEK", e);
            throw new RuntimeException(e);
        } finally {
            if (!success) {
                kmsMetrics.incFailedKeyDecrypt();
            }
        }
    }

    /**
     * Gets the traceId of the active {@link io.opentracing.Span} if available, otherwise generates a randomId.
     */
    protected String getRequestId() {
        String requestId;
        if (GlobalTracer.get().activeSpan() != null) {
            requestId = GlobalTracer.get().activeSpan().context().toTraceId();
        } else {
            // Generate a random ID faster than UUID's as it does not rely on java.security.SecureRandom.
            byte[] bytes = new byte[16];
            ThreadLocalRandom.current().nextBytes(bytes);
            requestId = ID_ENCODER.encodeToString(bytes);
        }
        return requestId;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(kmsKeyCache);
        IOUtils.closeQuietly(kmsClient);
    }

    /**
     * Creates a singleton {@link KmsKeySupplier} calling a singleton {@link KmsClient}.
     * This factory is used only once in {@link org.apache.solr.encryption.EncryptionDirectoryFactory}
     * to create a single {@link KmsKeySupplier}.
     */
    public static class Factory implements KeySupplier.Factory {

        /**
         * Required Solr config parameter to define the {@link KmsClient.Factory} class used
         * to create the {@link KmsClient} singleton.
         */
        public static final String PARAM_KMS_CLIENT_FACTORY = "kmsClientFactory";
        /**
         * Optional Solr config parameter to set the key cache expiration, in minutes.
         * The default expiration is {@link #KEY_CACHE_EXPIRATION}.
         */
        public static final String PARAM_KEY_CACHE_EXPIRATION = "keyCache.expirationMin";
        /** Default expiration of each key cache entry. */
        public static final Duration KEY_CACHE_EXPIRATION = Duration.ofMinutes(15);

        protected static final String KEY_SUPPLIER = KmsKeySupplier.class.getName();

        private KeySupplier keySupplier;

        @Override
        public void init(NamedList<?> args, CoreContainer coreContainer) {
            // The keySupplier is a singleton stored in the CoreContainer object cache.
            // It is Closeable, so it will be closed automatically when the CoreContainer closes the ObjectCache.
            keySupplier = coreContainer
                    .getObjectCache()
                    .computeIfAbsent(KEY_SUPPLIER, KeySupplier.class, k -> createKeySupplier(args, coreContainer));
        }

        private KeySupplier createKeySupplier(NamedList<?> args, CoreContainer coreContainer) {
            KmsMetrics kmsMetrics = null;
            KmsClient kmsClient = null;
            KmsKeyCache kmsKeyCache = null;
            boolean success = false;
            try {
                kmsMetrics = createEncryptionMetrics(coreContainer);
                KmsClient.Factory kmsClientFactory = getKmsClientFactory(args, coreContainer);
                kmsClient = kmsClientFactory.create(args);
                kmsKeyCache = createKeyCache(getKeyCacheExpiration(args));
                KeySupplier keySupplier = createKeySupplier(kmsClient, kmsKeyCache, kmsMetrics);
                log.info("KmsKeySupplier singleton created");
                success = true;
                return keySupplier;
            } catch (Throwable t) {
                // If something fails during the creation of the KMS client, return an InvalidKeySupplier.
                // That way, Solr can be used normally if the encryption is not used.
                // But any attempt to use the InvalidKeySupplier will throw an exception with the root cause.
                log.error("Failed to create the key supplier; encryption is not available", t);
                return new InvalidKeySupplier(t);
            } finally {
                if (!success) {
                    IOUtils.closeQuietly(kmsClient);
                    IOUtils.closeQuietly(kmsKeyCache);
                    if (kmsMetrics != null) {
                        kmsMetrics.incFailedKmsInit();
                    }
                }
            }
        }

        private KmsClient.Factory getKmsClientFactory(NamedList<?> args, CoreContainer coreContainer) {
            String kmsClientFactoryClass = args._getStr(PARAM_KMS_CLIENT_FACTORY, System.getProperty("solr." + PARAM_KMS_CLIENT_FACTORY));
            if (kmsClientFactoryClass == null) {
                throw new IllegalArgumentException("Missing " + PARAM_KMS_CLIENT_FACTORY + " argument for " + getClass().getName());
            }
            return coreContainer.getResourceLoader().newInstance(kmsClientFactoryClass, KmsClient.Factory.class);
        }

        protected Duration getKeyCacheExpiration(NamedList<?> args) {
            Duration keyCacheExpiration;
            String expirationString = (String) args.get(PARAM_KEY_CACHE_EXPIRATION);
            if (expirationString == null) {
                keyCacheExpiration = KEY_CACHE_EXPIRATION;
            } else {
                try {
                    keyCacheExpiration = Duration.ofMinutes(Long.parseLong(expirationString));
                } catch (NumberFormatException e) {
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                            "invalid integer value '" + expirationString
                                    + "' for parameter " + PARAM_KEY_CACHE_EXPIRATION);
                }
            }
            return keyCacheExpiration;
        }

        protected KmsKeyCache createKeyCache(Duration cacheExpiration) {
            return new KmsKeyCache(cacheExpiration, Ticker.systemTicker());
        }

        protected KmsMetrics createEncryptionMetrics(CoreContainer coreContainer) {
            return new KmsMetrics(coreContainer);
        }

        protected KeySupplier createKeySupplier(
                KmsClient kmsClient,
                KmsKeyCache kmsKeyCache,
                KmsMetrics kmsMetrics) {
            return new KmsKeySupplier(
                    kmsClient,
                    kmsKeyCache,
                    new ConcurrentHashMap<>(),
                    kmsMetrics,
                    Ticker.systemTicker());
        }

        @Override
        public KeySupplier create() throws IOException {
            // Return the KmsKeySupplier singleton instead of creating a new instance.
            return keySupplier;
        }

        /**
         * Provided when the construction of the {@link KmsKeySupplier} fails.
         */
        private static class InvalidKeySupplier implements KeySupplier {

            final Throwable cause;

            InvalidKeySupplier(Throwable cause) {
                this.cause = cause;
            }

            @Override
            public boolean shouldEncrypt(String fileName) {
                return KmsKeySupplier.shouldEncryptFile(fileName);
            }

            @Override
            public Map<String, String> getKeyCookie(String keyId, Map<String, String> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] getKeySecret(String keyId, Function<String, Map<String, String>> cookieSupplier) {
                throw new SolrException(
                        SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                        "Not available as KmsKeySupplier.Factory failed to initialize",
                        cause);
            }

            @Override
            public void close() {}
        }
    }
}
