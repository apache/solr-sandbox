/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.crossdc;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class TestMessageProcessor {
    static final String VERSION_FIELD = "_version_";

    private static class NoOpResubmitBackoffPolicy implements MessageProcessor.ResubmitBackoffPolicy {
        @Override
        public long getBackoffTimeMs(MirroredSolrRequest resubmitRequest) {
            return 0;
        }
    }

    @Mock
    private CloudSolrClient buddySolrClient;
    private MessageProcessor processor;

    private MessageProcessor.ResubmitBackoffPolicy backoffPolicy = spy(new NoOpResubmitBackoffPolicy());

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        processor = Mockito.spy(new MessageProcessor(buddySolrClient,
                backoffPolicy));
        Mockito.doNothing().when(processor).uncheckedSleep(anyLong());
    }

    @Test
    public void testSkipDeleteCollectionInLiveCluster() throws Exception {
        CollectionAdminRequest.Delete deleteRequest = CollectionAdminRequest.deleteCollection("test-delete-collection");
        IQueueHandler.Result<MirroredSolrRequest> result =
                processor.handleItem(new MirroredSolrRequest(1, deleteRequest, System.nanoTime()));
        assertEquals(IQueueHandler.ResultStatus.FAILED_NO_RETRY, result.status());
    }

    @Test
    public void testDoDeleteCollectionInNonLiveCluster() throws Exception {
        when(buddySolrClient.request(isA(SolrRequest.class), (String) isNull())).thenAnswer(invocation -> {
            SolrRequest req = (SolrRequest) invocation.getArguments()[0];
            SolrParams params = req.getParams();
            assertEquals(CollectionParams.CollectionAction.DELETE.name(), params.get(
                    CoreAdminParams.ACTION));
            assertEquals("test-collection", params.get("name"));


            NamedList<Object> response = new NamedList<>();
            NamedList<Object> responseHeader = new NamedList<>();
            responseHeader.add("status", 0);
            response.add("responseHeader", responseHeader);
            return response;
        });

        CollectionAdminRequest.Delete deleteRequest = CollectionAdminRequest.deleteCollection("test-collection");

        IQueueHandler.Result<MirroredSolrRequest> result =
                processor.handleItem(new MirroredSolrRequest(1, deleteRequest, System.nanoTime()));
        assertEquals(IQueueHandler.ResultStatus.HANDLED, result.status());
    }

    @Test
    public void testDocumentSanitization() throws Exception {
        UpdateRequest request = spy(new UpdateRequest());

        // Add docs with and without version
        request.add(new SolrInputDocument() {
            {
                setField("id", 1);
                setField(VERSION_FIELD, 1);
                setField("_expire_at_", "some time");
            }
        });
        request.add(new SolrInputDocument() {
            {
                setField("id", 2);
            }
        });

        // Delete by id with and without version
        request.deleteById("1");
        request.deleteById("2", 10L);

        // The response is irrelevant but it will fail because mocked server returns null when processing
        processor.handleItem(new MirroredSolrRequest(request));

        // After processing, check that all version fields are stripped
        for (SolrInputDocument doc : request.getDocuments()) {
            assertEquals("Doc still has version", null, doc.getField(VERSION_FIELD));
            assertEquals("Doc still has _expire_at_", null, doc.getField("_expire_at_"));
        }

        // Check versions in delete by id
        for (Map<String, Object> idParams : request.getDeleteByIdMap().values()) {
            if (idParams != null) {
                idParams.put(UpdateRequest.VER, null);
                assertEquals("Delete still has version", null, idParams.get(UpdateRequest.VER));
            }
        }
    }

    @Test
    public void testFailedResubmitBackoff() throws Exception {
        final UpdateRequest request = new UpdateRequest();
        when(buddySolrClient.request(eq(request), anyString())).thenThrow(new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "err msg", null));

        processor.handleItem(new MirroredSolrRequest(request));

        verify(backoffPolicy, times(1)).getBackoffTimeMs(any());
    }

    @Test
    public void testSuccessNoBackoff() throws Exception {
        final UpdateRequest request = spy(new UpdateRequest());
        when(buddySolrClient.request(eq(request), anyString())).thenReturn(new NamedList<>());

        processor.handleItem(new MirroredSolrRequest(request));

        verify(backoffPolicy, times(0)).getBackoffTimeMs(any());
    }

    @Test
    public void testClientErrorNoRetries() throws Exception {
        final UpdateRequest request = new UpdateRequest();
        when(buddySolrClient.request(eq(request), anyString())).thenThrow(
                new SolrException(
                        SolrException.ErrorCode.BAD_REQUEST, "err msg"));

        IQueueHandler.Result<MirroredSolrRequest> result = processor.handleItem(new MirroredSolrRequest(request));
        assertEquals(IQueueHandler.ResultStatus.FAILED_RESUBMIT, result.status());
    }
}
