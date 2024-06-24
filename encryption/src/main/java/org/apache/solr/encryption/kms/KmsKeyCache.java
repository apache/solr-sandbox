package org.apache.solr.encryption.kms;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.solr.common.util.SolrNamedThreadFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Cache of encryption keys. Keys are key ids and values are clear-text key secrets.
 * Entries are wiped automatically after a time-based expiration with a dedicated
 * thread which is stopped when {@link #close()} is called.
 */
public class KmsKeyCache implements Closeable {

    /**
     * Expected number of keys in the cache.
     * Internally, the Caffeine cache stores entries in a ConcurrentHashMap, which computes
     * the power-of-2 table size with (initialCapacity / loadFactor) + 1.
     */
    private static final int INITIAL_CAPACITY = 30;

    private final ScheduledExecutorService executorService;
    private final Cache<String, byte[]> keySecretCache;

    /**
     * @param cacheExpiration Each entry in the cache expires after this duration, and it is wiped.
     * @param ticker Ticker to use to get the wall clock time.
     */
    protected KmsKeyCache(Duration cacheExpiration, Ticker ticker) {
        executorService = Executors.newScheduledThreadPool(
                1,
                new DaemonThreadFactory("EncryptionKeyCache"));
        // We use a Caffeine cache not to limit the cache size and optimize the removal policy,
        // but rather to leverage the scheduled removal of expired entries, with a listener to
        // wipe the key secrets from the memory when they are removed.
        keySecretCache = Caffeine.newBuilder()
                // No maximum size. There will be one entry per active encryption key per
                // collection. We don't expect the size to go very high.
                .initialCapacity(INITIAL_CAPACITY)
                // Evict keys from the cache after expiration.
                .expireAfterWrite(cacheExpiration)
                .executor(executorService)
                .ticker(ticker)
                // Ensure time-based expiration by using a scheduler thread.
                .scheduler(Scheduler.forScheduledExecutorService(executorService))
                // Wipe the key secret bytes in memory when it is evicted from the cache.
                .removalListener(createCacheRemovalListener())
                .build();
    }

    protected RemovalListener<String, byte[]> createCacheRemovalListener() {
        return (keyId, secret, removalCause) -> {
            if (secret != null) {
                // Wipe the key secret bytes when an entry is removed, to guarantee
                // the key secret is not in memory anymore.
                Arrays.fill(secret, (byte) 0);
            }};
    }

    /**
     * @return The backing {@link Cache}.
     */
    protected Cache<String, byte[]> getCache() {
        return keySecretCache;
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    private static class DaemonThreadFactory extends SolrNamedThreadFactory {

        DaemonThreadFactory(String namePrefix) {
            super(namePrefix);
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setDaemon(true);
            return t;
        }
    }
}
