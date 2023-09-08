package org.apache.solr.handler.admin;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.crossdc.common.ConfigProperty;
import org.apache.solr.crossdc.common.CrossDcConstants;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.ConfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MirroringConfigSetsHandler extends ConfigSetsHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private KafkaMirroringSink sink;

  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public MirroringConfigSetsHandler(CoreContainer coreContainer) {
    this(coreContainer, null);
  }

  public MirroringConfigSetsHandler(CoreContainer coreContainer, KafkaMirroringSink sink) {
    super(coreContainer);
    log.info("Using MirroringCollectionsHandler.");
    if (sink == null) {
      Map<String, Object> properties = new HashMap<>();
      try {
        if (coreContainer.getZkController() != null) {
          ConfUtil.fillProperties(coreContainer.getZkController().getZkClient(), properties);
        }
        KafkaCrossDcConf conf = new KafkaCrossDcConf(properties);
        this.sink = new KafkaMirroringSink(conf);
      } catch (Exception e) {
        log.error("Exception configuring Kafka sink - mirroring disabled!", e);
        this.sink = null;
      }
    } else {
      this.sink = sink;
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
    // fully read all streams and re-package them so they are re-readable
    LocalSolrQueryRequest localReq = new LocalSolrQueryRequest(req.getCore(), req.getParams());
    List<ContentStream> contentStreams = null;
    if (req.getContentStreams() != null) {
      contentStreams = new ArrayList<>();
      for (ContentStream cs : req.getContentStreams()) {
        MirroredSolrRequest.ExposedByteArrayContentStream stream = MirroredSolrRequest.ExposedByteArrayContentStream.of(cs);
        contentStreams.add(stream);
      }
      localReq.setContentStreams(contentStreams);
    }
    // throw any errors before mirroring
    baseHandleRequestBody(localReq, rsp);

    if (rsp.getException() != null) {
      return;
    }
    if (sink == null) {
      return;
    }
    SolrRequest.METHOD method = SolrRequest.METHOD.valueOf(req.getHttpMethod().toUpperCase(Locale.ROOT));
    MirroredSolrRequest.MirroredConfigSetRequest configSetRequest = new MirroredSolrRequest.MirroredConfigSetRequest(method, req.getParams(), contentStreams);
    sink.submit(new MirroredSolrRequest(MirroredSolrRequest.Type.CONFIGSET, configSetRequest));
  }

  @VisibleForTesting
  public void baseHandleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    super.handleRequestBody(req, rsp);
  }
}
