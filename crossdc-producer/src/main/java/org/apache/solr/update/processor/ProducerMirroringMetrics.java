package org.apache.solr.update.processor;

import com.codahale.metrics.Counter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;

public class ProducerMirroringMetrics {

    private final Counter messagesCounter;
    private final Counter dlqMessagesCounter;
    private final Counter tooLargeDocumentsCounter;

    public ProducerMirroringMetrics(SolrMetricsContext solrMetricsContext, SolrCore solrCore) {
        messagesCounter = solrMetricsContext.counter(solrCore, "messagesCounter", "crossdc", "producer");
        dlqMessagesCounter = solrMetricsContext.counter(solrCore, "dlqMessagesCounter", "crossdc", "producer");
        tooLargeDocumentsCounter = solrMetricsContext.counter(solrCore, "tooLargeDocumentsCounter", "crossdc", "producer");
    }

    public Counter getMessagesCounter() {
        return this.messagesCounter;
    }

    public Counter getDlqMessagesCounter() {
        return this.dlqMessagesCounter;
    }

    public Counter getTooLargeDocumentsCounter() {
        return this.tooLargeDocumentsCounter;
    }
}
