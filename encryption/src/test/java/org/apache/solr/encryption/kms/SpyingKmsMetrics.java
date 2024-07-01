package org.apache.solr.encryption.kms;

import org.apache.solr.core.CoreContainer;

/**
 * Spies {@link KmsMetrics} and allows tests to reset them.
 */
public class SpyingKmsMetrics extends KmsMetrics {

    public SpyingKmsMetrics(CoreContainer coreContainer) {
        super(coreContainer);
    }

    public void reset() {
        numFailedKmsInit.dec(numFailedKmsInit.getCount());
        numKeyDecrypt.dec(numKeyDecrypt.getCount());
        numFailedKeyDecrypt.dec(numFailedKeyDecrypt.getCount());
        numSlowKeyDecrypt.dec(numSlowKeyDecrypt.getCount());
    }

    public long getNumFailedKmsInit() {
        return numFailedKmsInit.getCount();
    }

    public long getNumKeyDecrypt() {
        return numKeyDecrypt.getCount();
    }

    public long getNumFailedKeyDecrypt() {
        return numFailedKeyDecrypt.getCount();
    }
}
