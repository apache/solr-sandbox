package org.apache.solr.update.processor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;

public class ProducerMirroringMetrics {

    private final Counter savedDocuments;
    private final Counter messages;
    private final Counter tooLargeDocuments;
    private final Counter mirroredFailures;
    private final Histogram documentsSize;

    public ProducerMirroringMetrics(SolrMetricsContext solrMetricsContext, SolrCore solrCore) {
        this.savedDocuments = solrMetricsContext.counter(solrCore, "savedDocuments", "crossdc", "producer");
        this.messages = solrMetricsContext.counter(solrCore, "messages", "crossdc", "producer");
        this.tooLargeDocuments = solrMetricsContext.counter(solrCore, "tooLargeDocuments", "crossdc", "producer", "errors");
        this.mirroredFailures = solrMetricsContext.counter(solrCore, "mirroredFailures", "crossdc", "producer", "errors");
        this.documentsSize = solrMetricsContext.histogram(solrCore, "documentsSize", "crossdc", "producer");
    }

    public Counter getSavedDocuments() {
        return this.savedDocuments;
    }

    public Counter getMessages() {
        return this.messages;
    }

    public Counter getTooLargeDocuments() {
        return this.tooLargeDocuments;
    }

    public Counter getMirroredFailures() {
        return this.mirroredFailures;
    }

    public Histogram getDocumentsSize() {
        return this.documentsSize;
    }
}
