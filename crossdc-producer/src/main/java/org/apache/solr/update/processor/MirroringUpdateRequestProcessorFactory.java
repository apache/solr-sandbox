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
import org.apache.solr.crossdc.common.ConfigProperty;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.*;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.*;

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
    public static final NoOpUpdateRequestProcessor NO_OP_UPDATE_REQUEST_PROCESSOR =
        new NoOpUpdateRequestProcessor();

    // Flag for mirroring requests
    public static final String SERVER_SHOULD_MIRROR = "shouldMirror";

    /** This is instantiated in inform(SolrCore) and then shared by all processor instances - visible for testing */
    private volatile KafkaRequestMirroringHandler mirroringHandler;


    private boolean enabled = true;
    private boolean indexUnmirrorableDocs = false;
    private KafkaCrossDcConf conf;

    private final Map<String,Object> properties = new HashMap<>();

    @Override
    public void init(final NamedList args) {
        super.init(args);

        Boolean enabled = args.getBooleanArg("enabled");
        if (enabled != null && !enabled) {
            this.enabled = false;
        }

        final Boolean indexUnmirrorableDocsArg = args.getBooleanArg("indexUnmirrorableDocs");
        if (indexUnmirrorableDocsArg != null && indexUnmirrorableDocsArg) {
            this.indexUnmirrorableDocs = true;
        }

        for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
            String val = args._getStr(configKey.getKey(), null);
            if (val != null && !val.isBlank()) {
                properties.put(configKey.getKey(), val);
            }
        }
    }

    private static class MyCloseHook extends CloseHook {
        private final Closer closer;

        public MyCloseHook(Closer closer) {
            this.closer = closer;
        }

        @Override public void preClose(SolrCore core) {

        }

        @Override public void postClose(SolrCore core) {
            closer.close();
        }
    }

    private static class Closer {
        private final KafkaMirroringSink sink;

        public Closer(KafkaMirroringSink sink) {
            this.sink = sink;
        }

        public final void close() {
            try {
                this.sink.close();
            } catch (IOException e) {
                log.error("Exception closing sink", e);
            }
        }

    }

    @Override
    public void inform(SolrCore core) {
        log.info("KafkaRequestMirroringHandler inform enabled={}", this.enabled);

        if (!enabled) {
            return;
        }

        log.info("Producer startup config properties before adding additional properties from Zookeeper={}", properties);

        Properties zkProps = null;
        try {
            if (core.getCoreContainer().getZkController()
                .getZkClient().exists(System.getProperty(CrossDcConf.ZK_CROSSDC_PROPS_PATH,
                    CrossDcConf.CROSSDC_PROPERTIES), true)) {
                byte[] data = core.getCoreContainer().getZkController().getZkClient().getData(System.getProperty(CrossDcConf.ZK_CROSSDC_PROPS_PATH,
                    CrossDcConf.CROSSDC_PROPERTIES), null, null, true);

                if (data == null) {
                    log.error(CrossDcConf.CROSSDC_PROPERTIES + " file in Zookeeper has no data");
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, CrossDcConf.CROSSDC_PROPERTIES
                        + " file in Zookeeper has no data");
                }

                zkProps = new Properties();
                zkProps.load(new ByteArrayInputStream(data));

                KafkaCrossDcConf.readZkProps(properties, zkProps);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted looking for CrossDC configuration in Zookeeper", e);
            throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
        } catch (Exception e) {
            log.error("Exception looking for CrossDC configuration in Zookeeper", e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception looking for CrossDC configuration in Zookeeper", e);
        }

        if (properties.get(BOOTSTRAP_SERVERS) == null) {
            log.error(
                "boostrapServers not specified for producer in CrossDC configuration props={} zkProps={}",
                properties, zkProps);
           throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "boostrapServers not specified for producer");
       }
        
        if (properties.get(TOPIC_NAME) == null) {
            log.error(
                "topicName not specified for producer in CrossDC configuration props={} zkProps={}",
                properties, zkProps);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "topicName not specified for producer");
        }

        // load the request mirroring sink class and instantiate.
       // mirroringHandler = core.getResourceLoader().newInstance(RequestMirroringHandler.class.getName(), KafkaRequestMirroringHandler.class);

        conf = new KafkaCrossDcConf(properties);


        KafkaMirroringSink sink = new KafkaMirroringSink(conf);

        Closer closer = new Closer(sink);
        core.addCloseHook(new MyCloseHook(closer));

        mirroringHandler = new KafkaRequestMirroringHandler(sink);
    }

    private static Integer getIntegerPropValue(String name, Properties props) {
        String value = props.getProperty(name);
        if (value == null) {
            return null;
        }
        return Integer.parseInt(value);
    }

    @Override
    public UpdateRequestProcessor getInstance(final SolrQueryRequest req, final SolrQueryResponse rsp,
                                                final UpdateRequestProcessor next) {

        if (!enabled) {
            return NO_OP_UPDATE_REQUEST_PROCESSOR;
        }

        // if the class fails to initialize
        if (mirroringHandler == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "mirroringHandler is null");
        }

        // Check if mirroring is disabled in request params, defaults to true
        boolean doMirroring = req.getParams().getBool(SERVER_SHOULD_MIRROR, true);
        final long maxMirroringBatchSizeBytes = conf.getInt(MAX_REQUEST_SIZE_BYTES);

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

        return new MirroringUpdateProcessor(next, doMirroring, indexUnmirrorableDocs, maxMirroringBatchSizeBytes, mirroredParams,
                DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM)), doMirroring ? mirroringHandler : null);
    }

    private static class NoOpUpdateRequestProcessor extends UpdateRequestProcessor {
        NoOpUpdateRequestProcessor() {
            super(null);
        }
    }


}