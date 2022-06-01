package org.apache.solr.update.processor;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

public class MirroringUpdateProcessor extends UpdateRequestProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Flag indicating whether this instance creates and submits a mirrored request. This override is
   * necessary to prevent circular mirroring between coupled cluster running this processor.
   */
  private final boolean doMirroring;
  private final RequestMirroringHandler requestMirroringHandler;

  /**
   * The mirrored request starts as null, gets created and appended to at each process() call,
   * then submitted on finish().
   */
  private UpdateRequest mirrorRequest;
  private final SolrParams mirrorParams;

  /**
   * The distributed processor downstream from us so we can establish if we're running on a leader shard
   */
  private DistributedUpdateProcessor distProc;

  /**
   * Distribution phase of the incoming requests
   */
  private DistributedUpdateProcessor.DistribPhase distribPhase;

  public MirroringUpdateProcessor(final UpdateRequestProcessor next, boolean doMirroring,
      final SolrParams mirroredReqParams,
      final DistributedUpdateProcessor.DistribPhase distribPhase,
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
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "DistributedUpdateProcessor must follow "
              + MirroringUpdateProcessor.class.getSimpleName());
    }
  }

  private UpdateRequest createAndOrGetMirrorRequest() {
    if (mirrorRequest == null) {
      mirrorRequest = new UpdateRequest();
      mirrorRequest.setParams(new ModifiableSolrParams(mirrorParams));
    }
    if (log.isDebugEnabled())
      log.debug("createOrGetMirrorRequest={}",
          mirrorRequest);
    return mirrorRequest;
  }

  @Override public void processAdd(final AddUpdateCommand cmd) throws IOException {
    if (log.isDebugEnabled())
      log.debug("processAdd isLeader={} cmd={}", distProc.isLeader(), cmd);
    super.processAdd(cmd); // let this throw to prevent mirroring invalid reqs

    // submit only from the leader shards so we mirror each doc once
    if (doMirroring && distProc.isLeader()) {
      SolrInputDocument doc = cmd.getSolrInputDocument().deepCopy();
      doc.removeField(CommonParams.VERSION_FIELD); // strip internal doc version
      createAndOrGetMirrorRequest().add(doc, cmd.commitWithin, cmd.overwrite);
    }
  }

  @Override public void processDelete(final DeleteUpdateCommand cmd) throws IOException {
    if (log.isDebugEnabled())
      log.debug("processDelete doMirroring={} isLeader={} cmd={}", doMirroring, distProc.isLeader(), cmd);
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
        if (distribPhase == DistributedUpdateProcessor.DistribPhase.NONE) {
          createAndOrGetMirrorRequest().deleteByQuery(cmd.query);
        }
      }
    }
  }

  @Override public void processRollback(final RollbackUpdateCommand cmd) throws IOException {
    super.processRollback(cmd);
    // TODO: We can't/shouldn't support this ?
  }

  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    log.info("process commit cmd={}", cmd);
    if (next != null) next.processCommit(cmd);
  }

  @Override public void finish() throws IOException {
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
