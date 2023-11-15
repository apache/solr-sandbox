package org.apache.solr.update.processor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;

public class ProducerMirroringMetrics {

    private final Counter messagesCounter;
    private final Counter dlqMessagesCounter;
    private final Counter tooLargeDocumentsCounter;
    private final Histogram documentsSizeHistogram;

    public ProducerMirroringMetrics(SolrMetricsContext solrMetricsContext, SolrCore solrCore) {
        messagesCounter = solrMetricsContext.counter(solrCore, "messagesCounter", "crossdc", "producer");
        dlqMessagesCounter = solrMetricsContext.counter(solrCore, "dlqMessagesCounter", "crossdc", "producer");
        tooLargeDocumentsCounter = solrMetricsContext.counter(solrCore, "tooLargeDocumentsCounter", "crossdc", "producer");
        documentsSizeHistogram = solrMetricsContext.histogram(solrCore, "documentsSizeHistogram", "crossdc", "producer");
    }

    public void incrementMessagesCounter() {
        this.messagesCounter.inc();
    }

    public void incrementDlqMessagesCounter() {
        this.dlqMessagesCounter.inc();
    }

    public void incrementTooLargeDocumentsCounter() {
        this.tooLargeDocumentsCounter.inc();
    }

    public void updateDocumentsSizeHistogram(long value) {
        this.documentsSizeHistogram.update(value);
    }
}
