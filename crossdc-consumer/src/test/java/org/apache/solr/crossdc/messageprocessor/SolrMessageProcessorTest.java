package org.apache.solr.crossdc.messageprocessor;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.ResubmitBackoffPolicy;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SolrMessageProcessorTest {
    private SolrMessageProcessor solrMessageProcessor;
    private CloudSolrClient client;
    private ResubmitBackoffPolicy resubmitBackoffPolicy;

    @Before
    public void setUp() {
        client = mock(CloudSolrClient.class);
        resubmitBackoffPolicy = mock(ResubmitBackoffPolicy.class);
        solrMessageProcessor = new SolrMessageProcessor(client, resubmitBackoffPolicy);
    }

    /**
     * Should handle MirroredSolrRequest and return a failed result with no retry
     */
    @Test
    public void handleItemWithFailedResultNoRetry() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);

        SolrResponseBase solrResponseBase = mock(SolrResponseBase.class);
        when(solrRequest.process(client)).thenReturn(solrResponseBase);
        when(solrResponseBase.getResponse()).thenReturn(new NamedList<>());
        when(solrResponseBase.getStatus()).thenReturn(ErrorCode.BAD_REQUEST.code);
        when(client.request(any(SolrRequest.class))).thenReturn(new NamedList<>());

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.FAILED_RESUBMIT, result.status());
    }

    /**
     * Should handle MirroredSolrRequest and return a failed result with resubmit
     */
    @Test
    public void handleItemWithFailedResultResubmit() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);
        when(solrRequest.process(client))
                .thenThrow(new SolrException(ErrorCode.SERVER_ERROR, "Server error"));

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.FAILED_RESUBMIT, result.status());
        assertEquals(mirroredSolrRequest, result.newItem());
    }

    /**
     * Should handle MirroredSolrRequest and return a successful result
     */
    @Test
    public void handleItemWithSuccessfulResult() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        SolrResponseBase solrResponse = mock(SolrResponseBase.class);

        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);
        when(solrRequest.process(client)).thenReturn(solrResponse);
        when(solrResponse.getStatus()).thenReturn(0);

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.HANDLED, result.status());
        assertNull(result.newItem());
    }

    /**
     * Should connect to Solr if not connected and process the request
     */
    @Test
    public void connectToSolrIfNeededAndProcessRequest() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        SolrResponseBase solrResponse = mock(SolrResponseBase.class);

        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);
        when(solrRequest.process(client)).thenReturn(solrResponse);
        when(solrResponse.getStatus()).thenReturn(0);

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.HANDLED, result.status());
        verify(client, times(1)).connect();
        verify(solrRequest, times(1)).process(client);
    }
}