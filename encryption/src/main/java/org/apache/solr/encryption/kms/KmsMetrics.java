package org.apache.solr.encryption.kms;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.CoreContainer;

/**
 * Metrics for calls to the Key Management System.
 */
public class KmsMetrics {

    private static final String KMS_METRICS_SCOPE = "ADMIN./admin/encrypt.kms";

    private static final String KMS_INIT_METRIC = KMS_METRICS_SCOPE + ".init";
    private static final String KMS_INIT_NUM_ERRORS = KMS_INIT_METRIC + ".numErrors";

    private static final String KEY_DECRYPT_METRIC = KMS_METRICS_SCOPE + ".decrypt";
    private static final String KEY_DECRYPT_TOTAL = KEY_DECRYPT_METRIC + ".total";
    private static final String KEY_DECRYPT_NUM_ERRORS = KEY_DECRYPT_METRIC + ".numErrors";
    private static final String KEY_DECRYPT_NUM_SLOW = KEY_DECRYPT_METRIC + ".numSlowOps";

    protected final Counter numFailedKmsInit;
    protected final Counter numKeyDecrypt;
    protected final Counter numFailedKeyDecrypt;
    protected final Counter numSlowKeyDecrypt;

    public KmsMetrics(CoreContainer coreContainer) {
        MetricRegistry metricRegistry = coreContainer.getMetricManager().registry("solr.node");
        numFailedKmsInit = metricRegistry.counter(KMS_INIT_NUM_ERRORS);
        numKeyDecrypt = metricRegistry.counter(KEY_DECRYPT_TOTAL);
        numFailedKeyDecrypt = metricRegistry.counter(KEY_DECRYPT_NUM_ERRORS);
        numSlowKeyDecrypt = metricRegistry.counter(KEY_DECRYPT_NUM_SLOW);
    }

    public void incFailedKmsInit() {
        numFailedKmsInit.inc();
    }

    public void incKeyDecrypt() {
        numKeyDecrypt.inc();
    }

    public void incFailedKeyDecrypt() {
        numFailedKeyDecrypt.inc();
    }

    public void incSlowKeyDecrypt() {
        numSlowKeyDecrypt.inc();
    }
}
