package org.apache.solr.handler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.crossdc.common.ConfUtil;
import org.apache.solr.crossdc.common.ConfigProperty;
import org.apache.solr.crossdc.common.CrossDcConstants;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class MirroringSchemaHandler extends SchemaHandler {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private NamedList args;
    private KafkaCrossDcConf conf;
    private boolean enabled = true;
    private KafkaMirroringSink sink;

    private final Map<String, Object> properties = new HashMap<>();

    @Override
    public void init(final NamedList args) {
        super.init(args);

        this.args = args;
    }

    @Override
    public void inform(SolrCore core) {
        if (core.getCoreContainer().isZooKeeperAware()) {
            lookupPropertyOverridesInZk(core);
        } else {
            applyArgsOverrides();
            if (enabled) {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, getClass().getSimpleName() + " only supported in SolrCloud mode; please disable or remove from solrconfig.xml");
            }
            log.warn("Core '{}' was configured to use a disabled {}, but {} is only supported in SolrCloud deployments.  A NoOp processor will be used instead", core.getName(), this.getClass().getSimpleName(), this.getClass().getSimpleName());
        }

        if (!enabled) {
            return;
        }

        ConfUtil.verifyProperties(properties);

        // load the request mirroring sink class and instantiate.
        // mirroringHandler = core.getResourceLoader().newInstance(RequestMirroringHandler.class.getName(), KafkaRequestMirroringHandler.class);

        conf = new KafkaCrossDcConf(properties);


        sink = new KafkaMirroringSink(conf);

        MirroringSchemaHandler.Closer closer = new MirroringSchemaHandler.Closer(sink);
        core.addCloseHook(new MirroringSchemaHandler.MyCloseHook(closer));
    }

    private void applyArgsOverrides() {
        Boolean enabled = args.getBooleanArg("enabled");
        if (enabled != null && !enabled) {
            this.enabled = false;
        }
        for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
            String val = args._getStr(configKey.getKey(), null);
            if (val != null && !val.isBlank()) {
                properties.put(configKey.getKey(), val);
            }
        }
    }


    private static class MyCloseHook implements CloseHook {
        private final MirroringSchemaHandler.Closer closer;

        public MyCloseHook(MirroringSchemaHandler.Closer closer) {
            this.closer = closer;
        }

        @Override
        public void preClose(SolrCore core) {

        }

        @Override
        public void postClose(SolrCore core) {
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

    private void lookupPropertyOverridesInZk(SolrCore core) {
        log.info("Producer startup config properties before adding additional properties from Zookeeper={}", properties);

        try {
            SolrZkClient solrZkClient = core.getCoreContainer().getZkController().getZkClient();
            ConfUtil.fillProperties(solrZkClient, properties);
            applyArgsOverrides();
            CollectionProperties cp = new CollectionProperties(solrZkClient);
            Map<String, String> collectionProperties = cp.getCollectionProperties(core.getCoreDescriptor().getCollectionName());
            for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
                String val = collectionProperties.get("crossdc." + configKey.getKey());
                if (val != null && !val.isBlank()) {
                    properties.put(configKey.getKey(), val);
                }
            }
            String enabledVal = collectionProperties.get("crossdc.enabled");
            if (enabledVal != null) {
                if (Boolean.parseBoolean(enabledVal.toString())) {
                    this.enabled = true;
                } else {
                    this.enabled = false;
                }
            }
        } catch (Exception e) {
            log.error("Exception looking for CrossDC configuration in Zookeeper", e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception looking for CrossDC configuration in Zookeeper", e);
        }
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        boolean doMirroring = req.getParams().getBool(CrossDcConstants.SHOULD_MIRROR, true);
        if (!doMirroring) {
            log.info(" -- doMirroring=false, skipping...");
            super.handleRequestBody(req, rsp);
            return;
        }

        // throw any errors before mirroring
        baseHandleRequestBody(req, rsp);
        SolrRequest.METHOD method = SolrRequest.METHOD.valueOf(req.getHttpMethod().toUpperCase(Locale.ROOT));
        if (SolrRequest.METHOD.POST.equals(method)) {
            if (rsp.getException() != null) {
                return;
            }
            if (sink == null) {
                return;
            }
            CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
            String collection = coreDesc.getCollectionName();
            if (collection == null) {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not determine collection name for "
                        + MirroringSchemaHandler.class.getSimpleName() + ". Solr may not be running in cloud mode.");
            }

            // mirror
            ModifiableSolrParams mirroredParams = ModifiableSolrParams.of(req.getParams());
            mirroredParams.set("collection", collection);
            // remove internal version parameter
            mirroredParams.remove(CommonParams.VERSION_FIELD);
            // make sure to turn this off to prevent looping
            mirroredParams.set(CrossDcConstants.SHOULD_MIRROR, Boolean.FALSE.toString());
            log.info("  -- mirroring mirroredParams={}, original responseHeader={}, responseValues={}", mirroredParams, rsp.getResponseHeader(), rsp.getValues());

            MirroredSolrRequest.MirroredSchemaRequest schemaRequest = new MirroredSolrRequest.MirroredSchemaRequest(method, mirroredParams);
            sink.submit(new MirroredSolrRequest(MirroredSolrRequest.Type.SCHEMA, schemaRequest));
        }
    }

    @VisibleForTesting
    public void baseHandleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        super.handleRequestBody(req, rsp);
    }
}
