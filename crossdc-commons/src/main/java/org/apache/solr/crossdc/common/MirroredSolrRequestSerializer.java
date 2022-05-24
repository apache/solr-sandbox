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
package org.apache.solr.crossdc.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MirroredSolrRequestSerializer implements Serializer<MirroredSolrRequest>, Deserializer<MirroredSolrRequest> {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private boolean isKey;
    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public MirroredSolrRequest deserialize(String topic, byte[] data) {
        Map solrRequest;

        JavaBinCodec codec = new JavaBinCodec();
        ByteArrayInputStream bais = new ByteArrayInputStream(data);

        try {
            solrRequest = (Map) codec.unmarshal(bais);

            log.info("Deserialized class={} solrRequest={}", solrRequest.getClass().getName(), solrRequest);


        } catch (Exception e) {
            log.error("Exception unmarshalling JavaBin", e);
            throw new RuntimeException(e);
        }

        UpdateRequest updateRequest = new UpdateRequest();
        List docs = (List) solrRequest.get("docs");
        if (docs != null) {
            updateRequest.add(docs);
        }

        List deletes = (List) solrRequest.get("deletes");
        if (deletes != null) {
            updateRequest.deleteById(deletes);
        }

        Map params = (Map) solrRequest.get("params");
        if (params != null) {
            updateRequest.setParams(ModifiableSolrParams.of(new MapSolrParams(params)));
        }

        return new MirroredSolrRequest(updateRequest);
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param request  MirroredSolrRequest that needs to be serialized
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, MirroredSolrRequest request) {
        // TODO: add checks
        UpdateRequest solrRequest = (UpdateRequest) request.getSolrRequest();

        log.info("serialize request={} docs={}", solrRequest, solrRequest.getDocuments());

        JavaBinCodec codec = new JavaBinCodec(null);

        ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
        Map map = new HashMap();
        map.put("params", solrRequest.getParams().getMap());
        map.put("docs", solrRequest.getDocuments());

        // TODO
        //map.put("deletes", solrRequest.getDeleteByIdMap());
        map.put("deletes", solrRequest.getDeleteById());

        try {
            codec.marshal(map, baos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.byteArray();
    }

    /**
     * Close this serializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    public void close() {
        Serializer.super.close();
    }

    private static final class ExposedByteArrayOutputStream extends ByteArrayOutputStream {
        ExposedByteArrayOutputStream() {
            super();
        }

        byte[] byteArray() {
            return buf;
        }
    }
}
