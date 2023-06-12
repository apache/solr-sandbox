package org.apache.solr.update.processor;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.*;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.*;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

public class MirroringUpdateProcessor extends UpdateRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Flag indicating whether this instance creates and submits a mirrored request. This override is
     * necessary to prevent circular mirroring between coupled cluster running this processor.
     */
    private final boolean doMirroring;
    private final RequestMirroringHandler requestMirroringHandler;
    private final String defaultDBQMethod;

    /**
     * The mirrored request starts as null, gets created and appended to at each process() call,
     * then submitted on finish().
     */
    private UpdateRequest mirrorRequest;
    private long mirrorRequestBytes;
    private final SolrParams mirrorParams;


    /**
     * Controls whether docs exceeding the max-size (and thus cannot be mirrored) are indexed locally.
     */
    private final boolean indexUnmirrorableDocs;
    private final long maxMirroringBatchSizeBytes;


    /**
     * The distributed processor downstream from us so we can establish if we're running on a leader shard
     */
    //private DistributedUpdateProcessor distProc;

    /**
     * Distribution phase of the incoming requests
     */
    private DistributedUpdateProcessor.DistribPhase distribPhase;

    public enum DBQ_Method {
        CONVERT_NO_PAGING("convert_no_paging"),
        DEFAULT("default"),
        DELETE_BY_QUERY("delete_by_query"),
        DELETE_BY_QUERY_LOCAL("delete_by_query_local");

        private final String value;

        DBQ_Method(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static DBQ_Method fromString(String value) {
            if (value == null) {
                return DEFAULT;
            }
            for (DBQ_Method method : DBQ_Method.values()) {
                if (method.getValue().equals(value)) {
                    return method;
                }
            }
            throw new IllegalArgumentException("Invalid dbqMethod: " + value);
        }
    }


    public MirroringUpdateProcessor(final UpdateRequestProcessor next, boolean doMirroring,
                                    final boolean indexUnmirrorableDocs,
                                    final long maxMirroringBatchSizeBytes,
                                    final SolrParams mirroredReqParams,
                                    final DistributedUpdateProcessor.DistribPhase distribPhase,
                                    final RequestMirroringHandler requestMirroringHandler, String defaultDBQMethod) {
        super(next);
        this.doMirroring = doMirroring;
        this.indexUnmirrorableDocs = indexUnmirrorableDocs;
        this.maxMirroringBatchSizeBytes = maxMirroringBatchSizeBytes;
        this.mirrorParams = mirroredReqParams;
        this.distribPhase = distribPhase;
        this.requestMirroringHandler = requestMirroringHandler;
        this.defaultDBQMethod = defaultDBQMethod;
        // Find the downstream distributed update processor

    }

    private UpdateRequest createAndOrGetMirrorRequest() {
        if (mirrorRequest == null) {
            mirrorRequest = new UpdateRequest();
            mirrorRequest.setParams(new ModifiableSolrParams(mirrorParams));
            mirrorRequestBytes = 0L;
        }
        if (log.isDebugEnabled())
            log.debug("createOrGetMirrorRequest={}",
                    mirrorRequest);
        return mirrorRequest;
    }

    @Override
    public void processAdd(final AddUpdateCommand cmd) throws IOException {

        final SolrInputDocument doc = cmd.getSolrInputDocument().deepCopy();
        doc.removeField(CommonParams.VERSION_FIELD); // strip internal doc version
        final long estimatedDocSizeInBytes = ObjectSizeEstimator.estimate(doc);
        final boolean tooLargeForKafka = estimatedDocSizeInBytes > maxMirroringBatchSizeBytes;
        if (tooLargeForKafka && !indexUnmirrorableDocs) {
            log.warn("Skipping indexing of doc {} as it exceeds the doc-size limit ({} bytes) and is unmirrorable.", cmd.getPrintableId(), maxMirroringBatchSizeBytes);
        } else {
            super.processAdd(cmd); // let this throw to prevent mirroring invalid reqs
        }

        // submit only from the leader shards so we mirror each doc once
        boolean isLeader = isLeader(cmd.getReq(), cmd.getIndexedIdStr(), null, cmd.getSolrInputDocument());
        if (doMirroring && isLeader) {
            if (tooLargeForKafka) {
                log.error("Skipping mirroring of doc {} because estimated size exceeds batch size limit {} bytes", cmd.getPrintableId(), maxMirroringBatchSizeBytes);
            } else {
                createAndOrGetMirrorRequest().add(doc, cmd.commitWithin, cmd.overwrite);
                mirrorRequestBytes += estimatedDocSizeInBytes;
            }
        }

        if (log.isDebugEnabled())
            log.debug("processAdd isLeader={} cmd={}", isLeader, cmd);
    }

    @Override
    public void processDelete(final DeleteUpdateCommand cmd) throws IOException {
        String dbqMethod = cmd.getReq().getParams().get("dbqMethod");
        if (dbqMethod == null) {
            dbqMethod = defaultDBQMethod;
        }
        DBQ_Method dbqMethodEnum = DBQ_Method.fromString(dbqMethod);


        if (doMirroring && !cmd.isDeleteById() && !"*:*".equals(cmd.query)) {
            CloudDescriptor cloudDesc = cmd.getReq().getCore().getCoreDescriptor().getCloudDescriptor();
            String collection = cloudDesc.getCollectionName();
            HttpClient httpClient = cmd.getReq().getCore().getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();

            try (HttpSolrClient client = new HttpSolrClient.Builder(cmd.getReq().getCore().getCoreContainer().getZkController().getBaseUrl()).withHttpClient(httpClient).build()) {
                String uniqueField = cmd.getReq().getSchema().getUniqueKeyField().getName();
                switch (dbqMethodEnum) {
                    case DEFAULT:
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
                                log.debug("resp: cm={}, ncm={}, cnt={}, results={} ", cursorMark, nextCursorMark, cnt,
                                        rsp.getResults());
                                cnt++;
                            }

                            processDBQResults(client, collection, uniqueField, rsp);
                            if (cursorMark.equals(nextCursorMark)) {
                                done = true;
                            }
                            cursorMark = nextCursorMark;
                        }
                        break;
                    case CONVERT_NO_PAGING:
                        rows = 10000;
                        q = new SolrQuery(cmd.query).setRows(rows).setFields(uniqueField);
                        QueryResponse rsp = client.query(collection, q);

                        // Convert query to delete-by-id commands
                        for (SolrDocument result : rsp.getResults()) {
                            DeleteUpdateCommand deleteCmd = new DeleteUpdateCommand(cmd.getReq());
                            deleteCmd.setId((String) result.getFieldValue(uniqueField));
                            super.processDelete(deleteCmd); // Execute locally
                            createAndOrGetMirrorRequest().deleteById((String) result.getFieldValue(uniqueField)); // Send to kafka
                        }
                        break;
                    case DELETE_BY_QUERY:
                        super.processDelete(cmd); // Execute delete by query locally
                        createAndOrGetMirrorRequest().deleteByQuery(cmd.query); // Send to kafka
                        break;
                    case DELETE_BY_QUERY_LOCAL:
                        super.processDelete(cmd); // Execute delete by query locally only
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid dbqMethod: " + dbqMethod);
                }


                return;
            } catch (SolrServerException e) {
                throw new SolrException(SERVER_ERROR, e);
            }
        }

        super.processDelete(cmd); // let this throw to prevent mirroring invalid requests

        if (doMirroring) {
            boolean isLeader = false;
            if (cmd.isDeleteById()) {
                // deleteById requests runs once per leader, so we just submit the request from the leader shard
                isLeader = isLeader(cmd.getReq(), ((DeleteUpdateCommand) cmd).getId(), null != cmd.getRoute() ? cmd.getRoute() : cmd.getReq().getParams().get(
                        ShardParams._ROUTE_), null);
                if (isLeader) {
                    createAndOrGetMirrorRequest().deleteById(cmd.getId()); // strip versions from deletes
                }
                if (log.isDebugEnabled())
                    log.debug("processDelete doMirroring={} isLeader={} cmd={}", true, isLeader, cmd);
            } else {
                // DBQs are sent to each shard leader, so we mirror from the original node to only mirror once
                // In general there's no way to guarantee that these run identically on the mirror since there are no
                // external doc versions.
                // TODO: Can we actually support this considering DBQs aren't versioned.

                if (distribPhase == DistributedUpdateProcessor.DistribPhase.NONE) {
                    createAndOrGetMirrorRequest().deleteByQuery(cmd.query);
                }
                if (log.isDebugEnabled())
                    log.debug("processDelete doMirroring={} cmd={}", true, cmd);
            }

        }
    }

    private static void processDBQResults(SolrClient client, String collection, String uniqueField,
                                          QueryResponse rsp)
            throws SolrServerException, IOException {
        SolrDocumentList results = rsp.getResults();
        List<String> ids = new ArrayList<>(results.size());
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

    @Override
    public void processRollback(final RollbackUpdateCommand cmd) throws IOException {
        super.processRollback(cmd);
        // TODO: We can't/shouldn't support this ?
    }

    public void processCommit(CommitUpdateCommand cmd) throws IOException {
        log.debug("process commit cmd={}", cmd);
        if (next != null) next.processCommit(cmd);
    }

    @Override
    public final void finish() throws IOException {
        super.finish();

        if (doMirroring && mirrorRequest != null) {
            // We are configured to mirror, but short-circuit on batches we already know will fail (because they cumulatively
            // exceed the mirroring max-size)
            if (mirrorRequestBytes > maxMirroringBatchSizeBytes) {
                final String batchedIds = mirrorRequest.getDocuments().stream()
                        .map(doc -> doc.getField("id").getValue().toString())
                        .collect(Collectors.joining(", "));
                log.warn("Mirroring skipped for request because batch size {} bytes exceeds limit {} bytes.  IDs: {}",
                        mirrorRequestBytes, maxMirroringBatchSizeBytes, batchedIds);
                mirrorRequest = null;
                mirrorRequestBytes = 0L;
                return;
            }

            try {
                requestMirroringHandler.mirror(mirrorRequest);
                mirrorRequest = null; // so we don't accidentally submit it again
            } catch (Exception e) {
                log.error("mirror submit failed", e);
                throw new SolrException(SERVER_ERROR, "mirror submit failed", e);
            }
        }
    }

    // package private for testing
    static class ObjectSizeEstimator {
        /**
         * Sizes of primitive classes.
         */
        private static final Map<Class<?>, Integer> primitiveSizes = new IdentityHashMap<>();

        static {
            primitiveSizes.put(boolean.class, 1);
            primitiveSizes.put(Boolean.class, 1);
            primitiveSizes.put(byte.class, 1);
            primitiveSizes.put(Byte.class, 1);
            primitiveSizes.put(char.class, Character.BYTES);
            primitiveSizes.put(Character.class, Character.BYTES);
            primitiveSizes.put(short.class, Short.BYTES);
            primitiveSizes.put(Short.class, Short.BYTES);
            primitiveSizes.put(int.class, Integer.BYTES);
            primitiveSizes.put(Integer.class, Integer.BYTES);
            primitiveSizes.put(float.class, Float.BYTES);
            primitiveSizes.put(Float.class, Float.BYTES);
            primitiveSizes.put(double.class, Double.BYTES);
            primitiveSizes.put(Double.class, Double.BYTES);
            primitiveSizes.put(long.class, Long.BYTES);
            primitiveSizes.put(Long.class, Long.BYTES);
        }

        public static long estimate(SolrInputDocument doc) {
            if (doc == null) return 0L;
            long size = 0;
            for (SolrInputField inputField : doc.values()) {
                size += primitiveEstimate(inputField.getName(), 0L);
                size += estimate(inputField.getValue());
            }

            if (doc.hasChildDocuments()) {
                for (SolrInputDocument childDoc : doc.getChildDocuments()) {
                    size += estimate(childDoc);
                }
            }
            return size;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        static long estimate(Object obj) {
            if (obj instanceof SolrInputDocument) {
                return estimate((SolrInputDocument) obj);
            }

            if (obj instanceof Map) {
                return estimate((Map) obj);
            }

            if (obj instanceof Collection) {
                return estimate((Collection) obj);
            }

            return primitiveEstimate(obj, 0L);
        }

        private static long primitiveEstimate(Object obj, long def) {
            Class<?> clazz = obj.getClass();
            if (clazz.isPrimitive()) {
                return primitiveSizes.get(clazz);
            }
            if (obj instanceof String) {
                return ((String) obj).length() * Character.BYTES;
            }
            return def;
        }

        private static long estimate(Map<Object, Object> map) {
            if (map.isEmpty()) return 0;
            long size = 0;
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                size += primitiveEstimate(entry.getKey(), 0L);
                size += estimate(entry.getValue());
            }
            return size;
        }

        private static long estimate(@SuppressWarnings({"rawtypes"}) Collection collection) {
            if (collection.isEmpty()) return 0;
            long size = 0;
            for (Object obj : collection) {
                size += estimate(obj);
            }
            return size;
        }
    }
}
