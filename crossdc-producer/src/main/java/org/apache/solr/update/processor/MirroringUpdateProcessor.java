package org.apache.solr.update.processor;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.*;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

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
  //private DistributedUpdateProcessor distProc;

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

    super.processAdd(cmd); // let this throw to prevent mirroring invalid reqs

    // submit only from the leader shards so we mirror each doc once
    boolean isLeader = isLeader(cmd.getReq(),  cmd.getIndexedIdStr(), null, cmd.getSolrInputDocument());
    if (doMirroring && isLeader) {
      SolrInputDocument doc = cmd.getSolrInputDocument().deepCopy();
      doc.removeField(CommonParams.VERSION_FIELD); // strip internal doc version
      createAndOrGetMirrorRequest().add(doc, cmd.commitWithin, cmd.overwrite);
    }

    if (log.isDebugEnabled())
      log.debug("processAdd isLeader={} cmd={}", isLeader, cmd);
  }

  @Override public void processDelete(final DeleteUpdateCommand cmd) throws IOException {
    if (doMirroring && !cmd.isDeleteById() && !"*:*".equals(cmd.query)) {

      CloudDescriptor cloudDesc =
          cmd.getReq().getCore().getCoreDescriptor().getCloudDescriptor();
      String collection = cloudDesc.getCollectionName();

      HttpClient httpClient = cmd.getReq().getCore().getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();

      try (HttpSolrClient client =
          new HttpSolrClient.Builder(cmd.getReq().getCore().getCoreContainer().getZkController().getBaseUrl()).withHttpClient(httpClient).build()) {

        String uniqueField = cmd.getReq().getSchema().getUniqueKeyField().getName();

        int rows = Integer.getInteger("solr.crossdc.dbq_rows", 1000);
        SolrQuery q = new SolrQuery(cmd.query).setRows(rows).setSort(SolrQuery.SortClause.asc(uniqueField)).setFields(uniqueField);
        String cursorMark = CursorMarkParams.CURSOR_MARK_START;

        int cnt = 1;
        boolean done = false;
        while (!done) {
          q.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
          QueryResponse rsp =
              client.query(collection, q);
          String nextCursorMark = rsp.getNextCursorMark();

          if (log.isDebugEnabled()) {
            log.debug("resp: cm={}, ncm={}, cnt={}, results={} ", cursorMark, nextCursorMark, cnt++,
                rsp.getResults());
          }

          processDBQResults(client, collection, uniqueField, rsp);
          if (cursorMark.equals(nextCursorMark)) {
            done = true;
          }
          cursorMark = nextCursorMark;
        }
      } catch (SolrServerException e) {
        throw new SolrException(SERVER_ERROR, e);
      }

      return;
    }
    super.processDelete(cmd); // let this throw to prevent mirroring invalid requests

    if (doMirroring) {
      boolean isLeader = false;
      if (cmd.isDeleteById()) {
        DeleteUpdateCommand dcmd = (DeleteUpdateCommand)cmd;
        // deleteById requests runs once per leader, so we just submit the request from the leader shard
        isLeader = isLeader(cmd.getReq(),  dcmd.getId(), null != cmd.getRoute() ? cmd.getRoute() : cmd.getReq().getParams().get(
            ShardParams._ROUTE_), null);
        if (isLeader) {
          createAndOrGetMirrorRequest().deleteById(cmd.getId()); // strip versions from deletes
        }
        if (log.isDebugEnabled())
          log.debug("processDelete doMirroring={} isLeader={} cmd={}", doMirroring, isLeader, cmd);
      } else {
        // DBQs are sent to each shard leader, so we mirror from the original node to only mirror once
        // In general there's no way to guarantee that these run identically on the mirror since there are no
        // external doc versions.
        // TODO: Can we actually support this considering DBQs aren't versioned.

        if (distribPhase == DistributedUpdateProcessor.DistribPhase.NONE) {
          createAndOrGetMirrorRequest().deleteByQuery(cmd.query);
        }
        if (log.isDebugEnabled())
          log.debug("processDelete doMirroring={} cmd={}", doMirroring, cmd);
      }

    }
  }

  private void processDBQResults(SolrClient client, String collection, String uniqueField, QueryResponse rsp)
      throws SolrServerException, IOException {
    SolrDocumentList results = rsp.getResults();
    List<String> ids = new ArrayList<>();
    results.forEach(entries -> {
      String id = entries.getFirstValue(uniqueField).toString();
      ids.add(id);
    });
    if (ids.size() > 0) {
      client.deleteById(collection, ids);
    }
  }

  private boolean isLeader(SolrQueryRequest req, String id, String route, SolrInputDocument doc) {
    CloudDescriptor cloudDesc =
        req.getCore().getCoreDescriptor().getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    ClusterState clusterState =
        req.getCore().getCoreContainer().getZkController().getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    Slice slice = coll.getRouter().getTargetSlice(id, doc, route, req.getParams(), coll);

    if (slice == null) {
      // No slice found.  Most strict routers will have already thrown an exception, so a null return is
      // a signal to use the slice of this core.
      // TODO: what if this core is not in the targeted collection?
      String shardId = cloudDesc.getShardId();
      slice = coll.getSlice(shardId);
      if (slice == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No shard " + shardId + " in " + coll);
      }
    }
    String shardId = slice.getName();
    Replica leaderReplica = null;
    try {
      leaderReplica = req.getCore().getCoreContainer().getZkController().getZkStateReader().getLeaderRetry(collection, shardId);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
    return leaderReplica.getName().equals(cloudDesc.getCoreNodeName());
  }

  @Override public void processRollback(final RollbackUpdateCommand cmd) throws IOException {
    super.processRollback(cmd);
    // TODO: We can't/shouldn't support this ?
  }

  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    log.debug("process commit cmd={}", cmd);
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
