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

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
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

    @Override
    public void init(final NamedList args) {

        super.init(args);
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
        // load the request mirroring sink class and instantiate.
       // mirroringHandler = core.getResourceLoader().newInstance(RequestMirroringHandler.class.getName(), KafkaRequestMirroringHandler.class);


        // TODO: Setup Kafka properly
        final String topicName = System.getProperty("topicName");
        if (topicName == null) {
            throw new IllegalArgumentException("topicName not specified for producer");
        }
        final String boostrapServers = System.getProperty("bootstrapServers");
        if (boostrapServers == null) {
            throw new IllegalArgumentException("boostrapServers not specified for producer");
        }
        KafkaCrossDcConf conf = new KafkaCrossDcConf(boostrapServers, topicName, false, null);
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
        log.info("Create MirroringUpdateProcessor");
        return new MirroringUpdateProcessor(next, doMirroring, mirroredParams,
                DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM)), doMirroring ? mirroringHandler : null);
    }

    public static class MirroringUpdateProcessor extends UpdateRequestProcessor {
        /** Flag indicating whether this instance creates and submits a mirrored request. This override is
         * necessary to prevent circular mirroring between coupled cluster running this processor. */
        private final boolean doMirroring;
        private final RequestMirroringHandler requestMirroringHandler;

        /** The mirrored request starts as null, gets created and appended to at each process() call,
         * then submitted on finish(). */
        private UpdateRequest mirrorRequest;
        private final SolrParams mirrorParams;

        /** The distributed processor downstream from us so we can establish if we're running on a leader shard */
        private DistributedUpdateProcessor distProc;

        /** Distribution phase of the incoming requests */
        private DistribPhase distribPhase;

        public MirroringUpdateProcessor(final UpdateRequestProcessor next, boolean doMirroring,
                                        final SolrParams mirroredReqParams, final DistribPhase distribPhase,
                                        final RequestMirroringHandler requestMirroringHandler) {
            super(next);
            this.doMirroring = doMirroring;
            this.mirrorParams = mirroredReqParams;
            this.distribPhase = distribPhase;
            this.requestMirroringHandler = requestMirroringHandler;

            // Find the downstream distributed update processor
            for (UpdateRequestProcessor proc = next; proc != null; proc = proc.next) {
                if (proc instanceof DistributedUpdateProcessor) {
                    distProc = (DistributedUpdateProcessor) proc;
                    break;
                }
            }
            if (distProc == null) {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "DistributedUpdateProcessor must follow "
                        + MirroringUpdateProcessor.class.getSimpleName());
            }
        }

        private UpdateRequest createAndOrGetMirrorRequest() {
            if (mirrorRequest == null) {
                mirrorRequest = new UpdateRequest();
                mirrorRequest.setParams(new ModifiableSolrParams(mirrorParams));
            }
            if (log.isDebugEnabled()) log.debug("createOrGetMirrorRequest={}", mirrorRequest);
            return mirrorRequest;
        }

        @Override
        public void processAdd(final AddUpdateCommand cmd) throws IOException {
            if (log.isDebugEnabled()) log.debug("processAdd isLeader={} cmd={}", distProc.isLeader(), cmd);
            super.processAdd(cmd); // let this throw to prevent mirroring invalid reqs

            // submit only from the leader shards so we mirror each doc once
            if (doMirroring && distProc.isLeader()) {
                SolrInputDocument doc = cmd.getSolrInputDocument().deepCopy();
                doc.removeField(CommonParams.VERSION_FIELD); // strip internal doc version
                createAndOrGetMirrorRequest().add(doc, cmd.commitWithin, cmd.overwrite);
            }
        }

        @Override
        public void processDelete(final DeleteUpdateCommand cmd) throws IOException {
            if (log.isDebugEnabled()) log.debug("processDelete doMirroring={} isLeader={} cmd={}", doMirroring, distProc.isLeader(), cmd);
            super.processDelete(cmd); // let this throw to prevent mirroring invalid requests

            if (doMirroring) {
                if (cmd.isDeleteById()) {
                    // deleteById requests runs once per leader, so we just submit the request from the leader shard
                    if (distProc.isLeader()) {
                        createAndOrGetMirrorRequest().deleteById(cmd.getId()); // strip versions from deletes
                    }
                } else {
                    // DBQs are sent to each shard leader, so we mirror from the original node to only mirror once
                    // In general there's no way to guarantee that these run identically on the mirror since there are no
                    // external doc versions.
                    // TODO: Can we actually support this considering DBQs aren't versioned.
                    if (distribPhase == DistribPhase.NONE) {
                        createAndOrGetMirrorRequest().deleteByQuery(cmd.query);
                    }
                }
            }
        }

        @Override
        public void processRollback(final RollbackUpdateCommand cmd) throws IOException {
            super.processRollback(cmd);
            // TODO: We can't/shouldn't support this ?
        }

        public void processCommit(CommitUpdateCommand cmd) throws IOException {
            log.info("process commit cmd={}", cmd);
            if (next != null) next.processCommit(cmd);
        }

        @Override
        public void finish() throws IOException {
            super.finish();

            if (doMirroring && mirrorRequest != null) {
                try {
                    requestMirroringHandler.mirror(mirrorRequest);
                    mirrorRequest = null; // so we don't accidentally submit it again
                } catch (Exception e) {
                    log.error("mirror submit failed", e);
                    throw new SolrException(SERVER_ERROR, "mirror submit failed", e);
                }
            }
        }
    }
}