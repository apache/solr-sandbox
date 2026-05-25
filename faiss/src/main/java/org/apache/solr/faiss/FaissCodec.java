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
package org.apache.solr.faiss;

import java.lang.invoke.MethodHandles;
import java.util.Locale;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Codec that uses FaissKnnVectorsFormat for FAISS-based vector search.
 *
 * @since 10.0.0
 */
public class FaissCodec extends FilterCodec {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String FAISS_ALGORITHM = "faiss";
  private static final String DEFAULT_FAISS_DESCRIPTION =
      String.format(Locale.ROOT, "IDMap,HNSW%d", HnswGraphBuilder.DEFAULT_MAX_CONN);
  private static final String DEFAULT_FAISS_INDEX_PARAMS =
      String.format(Locale.ROOT, "efConstruction=%d", HnswGraphBuilder.DEFAULT_BEAM_WIDTH);

  private final SolrCore core;
  private final Codec fallbackCodec;
  private final FaissKnnVectorsFormat faissKnnVectorsFormat;

  public FaissCodec(SolrCore core, Codec fallback, NamedList<?> args) {
    super(fallback.getName(), fallback);
    this.core = core;
    this.fallbackCodec = fallback;

    String descriptionStr = (String) args.get("faissDescription");
    String description = descriptionStr != null ? descriptionStr : DEFAULT_FAISS_DESCRIPTION;
    String indexParamsStr = (String) args.get("faissIndexParams");
    String indexParams = indexParamsStr != null ? indexParamsStr : DEFAULT_FAISS_INDEX_PARAMS;

    faissKnnVectorsFormat = new FaissKnnVectorsFormat(description, indexParams);

    if (log.isInfoEnabled()) {
      log.info(
          "FaissKnnVectorsFormat initialized with parameter values: description={}, indexParams={}",
          description,
          indexParams);
    }
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return perFieldKnnVectorsFormat;
  }

  private PerFieldKnnVectorsFormat perFieldKnnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          final SchemaField schemaField = core.getLatestSchema().getFieldOrNull(field);
          FieldType fieldType = (schemaField == null ? null : schemaField.getType());
          if (fieldType instanceof DenseVectorField vectorType) {
            String knnAlgorithm = vectorType.getKnnAlgorithm();
            if (FAISS_ALGORITHM.equals(knnAlgorithm)) {
              return faissKnnVectorsFormat;
            } else if (DenseVectorField.HNSW_ALGORITHM.equals(knnAlgorithm)) {
              return fallbackCodec.knnVectorsFormat();
            } else {
              throw new SolrException(
                  SolrException.ErrorCode.SERVER_ERROR,
                  knnAlgorithm + " KNN algorithm is not supported");
            }
          }
          return fallbackCodec.knnVectorsFormat();
        }
      };
}

