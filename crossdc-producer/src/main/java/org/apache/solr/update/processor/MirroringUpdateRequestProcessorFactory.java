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
package org.apache.solr.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.*;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * An update processor that works with the {@link UpdateRequestProcessorFactory} to mirror update requests by
 * submitting them to a sink that implements a queue producer.
 *
 * ADDs and DeleteByIDs are mirrored from leader shards and have internal _version_ fields stripped.
 * node.
 *
 * A single init arg is required, <b>requestMirroringHandler</b>, which specifies the plugin class used for mirroring
 * requests. This class must implement {@link RequestMirroringHandler}.
 *
 * It is recommended to use the {@link DocBasedVersionConstraintsProcessorFactory} upstream of this factory to ensure
 * doc consistency between this cluster and the mirror(s).
 */
public class MirroringUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory
        implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Flag for mirroring requests
    public static String SERVER_SHOULD_MIRROR = "shouldMirror";

    /** This is instantiated in inform(SolrCore) and then shared by all processor instances - visible for testing */
    volatile KafkaRequestMirroringHandler mirroringHandler;
    private String topicName;
    private String bootstrapServers;

    @Override
    public void init(final NamedList args) {
        super.init(args);

        topicName = args._getStr("topicName", null);
        bootstrapServers = args._getStr("bootstrapServers", null);
    }

    private class Closer {
        private final KafkaMirroringSink sink;

        public Closer(KafkaMirroringSink sink) {
            this.sink = sink;
        }

        public void close() {
            try {
                this.sink.close();
            } catch (IOException e) {
                log.error("Exception closing sink", sink);
            }
        }

    }

    @Override
    public void inform(SolrCore core) {
        log.info("KafkaRequestMirroringHandler inform");

        try {
            if ((topicName == null || topicName.isBlank()) || (bootstrapServers == null || bootstrapServers.isBlank()) && core.getCoreContainer().getZkController()
                .getZkClient().exists(CrossDcConf.CROSSDC_PROPERTIES, true)) {
                byte[] data = core.getCoreContainer().getZkController().getZkClient().getData("/crossdc.properties", null, null, true);
                Properties props = new Properties();
                props.load(new ByteArrayInputStream(data));

                if (topicName == null || topicName.isBlank()) {
                    topicName = props.getProperty("topicName");
                }
                if (bootstrapServers == null || bootstrapServers.isBlank()) {
                    bootstrapServers = props.getProperty("bootstrapServers");
                }
             }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
        } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }

        if (bootstrapServers == null || bootstrapServers.isBlank()) {
           throw new IllegalArgumentException("boostrapServers not specified for producer");
       }
        
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName not specified for producer");
        }

        log.info("bootstrapServers={} topicName={}", bootstrapServers, topicName);

        // load the request mirroring sink class and instantiate.
       // mirroringHandler = core.getResourceLoader().newInstance(RequestMirroringHandler.class.getName(), KafkaRequestMirroringHandler.class);

        KafkaCrossDcConf conf = new KafkaCrossDcConf(bootstrapServers, topicName, false, null);
        KafkaMirroringSink sink = new KafkaMirroringSink(conf);

        Closer closer = new Closer(sink);
        core.addCloseHook(new CloseHook() {
            @Override public void preClose(SolrCore core) {

            }

            @Override public void postClose(SolrCore core) {
                closer.close();
            }
        });

        mirroringHandler = new KafkaRequestMirroringHandler(sink);
    }

    @Override
    public MirroringUpdateProcessor getInstance(final SolrQueryRequest req, final SolrQueryResponse rsp,
                                                final UpdateRequestProcessor next) {
        // if the class fails to initialize
        if (mirroringHandler == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "mirroringHandler is null");
        }

        // Check if mirroring is disabled in request params, defaults to true
        boolean doMirroring = req.getParams().getBool(SERVER_SHOULD_MIRROR, true);

        ModifiableSolrParams mirroredParams = null;
        if (doMirroring) {
            // Get the collection name for the core so we can be explicit in the mirrored request
            CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
            String collection = coreDesc.getCollectionName();
            if (collection == null) {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not determine collection name for "
                        + MirroringUpdateProcessor.class.getSimpleName() + ". Solr may not be running in cloud mode.");
            }

            mirroredParams = new ModifiableSolrParams(req.getParams());
            mirroredParams.set("collection", collection);
            // remove internal version parameter
            mirroredParams.remove(CommonParams.VERSION_FIELD);
            // remove fields added by distributed update proc
            mirroredParams.remove(DISTRIB_UPDATE_PARAM);
            mirroredParams.remove(DISTRIB_FROM_COLLECTION);
            mirroredParams.remove(DISTRIB_INPLACE_PREVVERSION);
            mirroredParams.remove(COMMIT_END_POINT);
            mirroredParams.remove(DISTRIB_FROM_SHARD);
            mirroredParams.remove(DISTRIB_FROM_PARENT);
            mirroredParams.remove(DISTRIB_FROM);
            // prevent circular mirroring
            mirroredParams.set(SERVER_SHOULD_MIRROR, Boolean.FALSE.toString());
        }
        if (log.isTraceEnabled()) {
            log.trace("Create MirroringUpdateProcessor with mirroredParams={}", mirroredParams);
        }
        log.info("Create MirroringUpdateProcessor");
        return new MirroringUpdateProcessor(next, doMirroring, mirroredParams,
                DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM)), doMirroring ? mirroringHandler : null);
    }

}