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
package org.apache.solr.encryption;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.TIME_ALLOWED;
import static org.apache.solr.encryption.CommitUtil.readLatestCommit;
import static org.apache.solr.encryption.EncryptionUtil.*;

/**
 * Handles an encryption request for a specific {@link SolrCore}.
 * <p>
 * The caller provides the mandatory {@link #PARAM_KEY_ID} request parameter to define the encryption
 * key id to use to encrypt the index files. To decrypt the index to cleartext, the special parameter
 * value {@link #NO_KEY_ID} must be provided.
 * <p>
 * The encryption processing is asynchronous. The request returns immediately with two response
 * parameters. {@link #ENCRYPTION_STATE} parameter with values {@link State#PENDING},
 * {@link State#COMPLETE}, or {@link State#BUSY}. And {@link #STATUS} parameter with values
 * {@link #STATUS_SUCCESS} or {@link #STATUS_FAILURE}.
 * <p>
 * The expected usage of this handler is to first send an encryption request with a key id, and
 * receive a response with {@link #STATUS_SUCCESS} and a {@link State#PENDING}. If the caller needs
 * to know when the encryption is complete, it can (optionally) repeatedly send the same encryption
 * request with the same key id, until it receives a response with {@link #STATUS_SUCCESS} and a
 * {@link State#COMPLETE}.
 * <p>
 * If the handler returns a response with {@link State#BUSY}, it means that another encryption for a
 * different key id is ongoing on the same Solr core. It cannot start a new encryption until it finishes.
 * <p>
 * If the handler returns a response with {@link #STATUS_FAILURE}, it means the request did not succeed
 * and should be retried by the caller (there should be error logs).
 * <p>
 * The caller can provide the additional parameter {@link org.apache.solr.common.params.CommonParams#DISTRIB} with
 * value "true". In this case, this handler will distribute the encryption request to all the leader replicas of
 * all the shards of the collection, ensuring they all encrypt their index shard. The response {@link #ENCRYPTION_STATE}
 * will be {@link State#COMPLETE} only when all the shards return {@link State#COMPLETE}. So the caller may repeatedly
 * send the same encryption request with {@link org.apache.solr.common.params.CommonParams#DISTRIB} "true"
 * to know when the whole collection encryption is complete. In addition, the caller can set the
 * {@link org.apache.solr.common.params.CommonParams#TIME_ALLOWED} parameter to define a timeout for the request
 * distribution, in milliseconds. This timeout is for the distribution itself, not the encryption process.
 */
public class EncryptionRequestHandler extends RequestHandlerBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Key id request parameter - required.
   * Its value should be {@link #NO_KEY_ID} for no key.
   */
  public static final String PARAM_KEY_ID = "encryptionKeyId";
  /**
   * Value of the key id parameter meaning there is no key and no encryption.
   */
  public static final String NO_KEY_ID = "no_key_id";

  /**
   * Encryption pending boolean parameter, in the commit user data.
   * When present and true, this means the encryption process is still pending.
   * It includes the {@link EncryptionUpdateHandler#TRANSFERABLE_COMMIT_DATA} prefix to be transferred from a
   * commit to the next one automatically.
   */
  private static final String COMMIT_ENCRYPTION_PENDING = COMMIT_CRYPTO + "encryptionPending";

  /**
   * Status of the request.
   */
  public static final String STATUS = "status";
  /**
   * One of {@link #STATUS} values: the request was processed successfully. Get additional information
   * with the {@link #ENCRYPTION_STATE} response parameter.
   */
  public static final String STATUS_SUCCESS = "success";
  /**
   * One of {@link #STATUS} values: the request was not processed correctly, an error occurred.
   */
  public static final String STATUS_FAILURE = "failure";

  /**
   * Response parameter name to provide the status of the encryption.
   */
  public static final String ENCRYPTION_STATE = "encryptionState";
  /**
   * Enumeration of the {@link #ENCRYPTION_STATE} values.
   */
  public enum State {
    /** The encryption with the provided key id is complete. */
    COMPLETE("complete", 0),
    /** The request distribution timed out (can only be returned when {@link org.apache.solr.common.params.CommonParams#DISTRIB} is set). */
    TIMEOUT("timeout", 1),
    /** The encryption with the provided key id is ongoing and pending. */
    PENDING("pending", 2),
    /** Another encryption for a different key id is ongoing on the same Solr core; cannot start a new encryption until it finishes. */
    BUSY("busy", 3),
    /** At least one distributed encryption request failed for a shard (can only be returned when {@link org.apache.solr.common.params.CommonParams#DISTRIB} is set). */
    ERROR("error", 4);

    public final String value;
    private final int priority;

    State(String value, int priority) {
      this.value = value;
      this.priority = priority;
    }

    public static State fromValue(String value) {
      switch (value) {
        case "complete":
          return COMPLETE;
        case "timeout":
          return TIMEOUT;
        case "pending":
          return PENDING;
        case "busy":
          return BUSY;
        case "error":
          return ERROR;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported encryption state '" + value + "'");
      }
    }

    public boolean isSuccess() {
      switch (this) {
        case COMPLETE:
        case PENDING:
        case BUSY:
          return true;
        default:
          return false;
      }
    }
  }

  private static final long DISTRIBUTION_MAX_ATTEMPTS = 3;

  private static final Object pendingEncryptionLock = new Object();
  private static final Map<String, PendingKeyId> pendingEncryptions = new HashMap<>();

  private final ExecutorService executor = ExecutorUtil.newMDCAwareFixedThreadPool(4, new ThreadFactory() {
    private int threadNum;

    @Override
    public synchronized Thread newThread(Runnable r) {
      Thread t = new Thread(r, "Encryption-" + threadNum++);
      t.setDaemon(true);
      return t;
    }
  });

  /**
   * Builds the key cookie based on the request.
   * After this method returns, the returned key cookie parameters will be stored in the index metadata along with the
   * key id. Later the key cookie will be passed to
   * {@link KeySupplier#getKeySecret(String, java.util.function.Function)} to get the key secret.
   * <p>
   * The default behavior is to call {@link KeySupplier#getKeyCookie(String, Map)} with no parameters.
   * Override this method to provide specific parameters to the {@link KeySupplier}.
   * <p>
   * If a required param is missing, this method should throw a {@link SolrException} with
   * {@link SolrException.ErrorCode#BAD_REQUEST} and should set the response status to {@link #STATUS_FAILURE}.
   *
   * @return the cookie parameters. The returned map is considered immutable and is never modified. This method may
   * return null or an empty map if no parameters.
   */
  @Nullable
  protected Map<String, String> buildKeyCookie(String keyId,
                                               SolrQueryRequest req,
                                               SolrQueryResponse rsp) throws IOException {
    KeySupplier keySupplier = EncryptionDirectoryFactory.getFactory(req.getCore()).getKeySupplier();
    return keySupplier.getKeyCookie(keyId, null);
  }

  @Override
  public void close() throws IOException {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      super.close();
    }
  }

  @Override
  public String getDescription() {
    return "Handles encryption requests";
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.CORE_EDIT_PERM;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    long startTimeNs = System.nanoTime();
    String keyId = req.getParams().get(PARAM_KEY_ID);
    if (keyId == null || keyId.isEmpty()) {
      rsp.add(STATUS, STATUS_FAILURE);
      throw new IOException("Parameter " + PARAM_KEY_ID + " must be present and not empty."
                              + " Use [" + PARAM_KEY_ID + "=\"" + NO_KEY_ID + "\"] for explicit decryption.");
    } else if (keyId.equals(NO_KEY_ID)) {
      keyId = null;
    }
    // Check the defined DirectoryFactory instance.
    EncryptionDirectoryFactory.getFactory(req.getCore());

    if (req.getParams().getBool(DISTRIB, false)) {
      distributeRequest(req, rsp, keyId, startTimeNs);
      return;
    }

    log.debug("Encrypt request for keyId={}", keyId);
    boolean success = false;
    State state = State.PENDING;
    try {
      SegmentInfos segmentInfos = readLatestCommit(req.getCore());
      if (segmentInfos.size() == 0) {
        commitEmptyIndexForEncryption(keyId, segmentInfos, req, rsp);
        state = State.COMPLETE;
        success = true;
        return;
      }

      boolean encryptionComplete = false;
      if (isCommitActiveKeyId(keyId, segmentInfos)) {
        log.debug("Provided keyId={} is the current active key id", keyId);
        if (Boolean.parseBoolean(segmentInfos.getUserData().get(COMMIT_ENCRYPTION_PENDING))) {
          encryptionComplete = areAllSegmentsEncryptedWithKeyId(keyId, req.getCore(), segmentInfos)
            && areAllLogsEncryptedWithKeyId(keyId, req.getCore(), segmentInfos);
          if (encryptionComplete) {
            commitEncryptionComplete(keyId, segmentInfos, req);
          }
        } else {
          encryptionComplete = true;
        }
      }
      if (encryptionComplete) {
        state = State.COMPLETE;
        success = true;
        return;
      }

      synchronized (pendingEncryptionLock) {
        PendingKeyId pendingKeyId = pendingEncryptions.get(req.getCore().getName());
        if (pendingKeyId != null) {
          if (Objects.equals(pendingKeyId.keyId, keyId)) {
            log.debug("Ongoing encryption for keyId={}", keyId);
            success = true;
          } else {
            log.debug("Core busy encrypting for keyId={} different than requested keyId={}",
                      pendingKeyId.keyId, keyId);
            state = State.BUSY;
          }
          return;
        }
        pendingEncryptions.put(req.getCore().getName(), new PendingKeyId(keyId));
      }
      try {
        commitEncryptionStart(keyId, segmentInfos, req, rsp);
        encryptAsync(req, startTimeNs);
        success = true;
      } finally {
        if (!success) {
          synchronized (pendingEncryptionLock) {
            pendingEncryptions.remove(req.getCore().getName());
          }
        }
      }

    } finally {
      if (success) {
        rsp.add(STATUS, STATUS_SUCCESS);
      } else {
        rsp.add(STATUS, STATUS_FAILURE);
      }
      rsp.add(ENCRYPTION_STATE, state.value);
      long timeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs);
      log.info("Responding encryption state={} success={} for keyId={} timeMs={}",
          state.value, success, keyId, timeMs);
    }
  }

  private void distributeRequest(SolrQueryRequest req, SolrQueryResponse rsp, String keyId, long startTimeNs) {
    boolean success = false;
    String collectionName = null;
    State collectionState = null;
    long timeAllowedMs = req.getParams().getLong(TIME_ALLOWED, 0);
    long maxTimeNs = timeAllowedMs <= 0 ? Long.MAX_VALUE : startTimeNs + timeAllowedMs;
    try {
      collectionName = req.getCore().getCoreDescriptor().getCollectionName();
      if (collectionName == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parameter " + DISTRIB + " can only be used in Solr Cloud mode.");
      }
      log.debug("Encrypt request distributed for keyId={} collection={}", keyId, collectionName);
      DocCollection docCollection = req.getCore().getCoreContainer().getZkController().getZkStateReader().getCollection(collectionName);
      if (docCollection == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parameter " + DISTRIB + " present but collection '" + collectionName + "' not found.");
      }
      try (SolrClientHolder solrClient = getHttpSolrClient(req)) {
        ModifiableSolrParams params = createDistributedRequestParams(req, rsp, keyId);
        for (Slice slice : docCollection.getActiveSlices()) {
          if (isTimeout(maxTimeNs)) {
            log.warn("Timeout distributing encryption request for keyId={} collection={}", keyId, collectionName);
            if (collectionState == null || State.TIMEOUT.priority > collectionState.priority) {
              collectionState = State.TIMEOUT;
            }
            break;
          }
          Replica replica = slice.getLeader();
          if (replica == null) {
            log.error("No leader found for shard {}", slice.getName());
            collectionState = State.ERROR;
            continue;
          }
          State state = sendEncryptionRequestWithRetry(replica, params, solrClient.getClient(), keyId, collectionName);
          if (collectionState == null || state.priority > collectionState.priority) {
            collectionState = state;
          }
        }
        success = collectionState == null || collectionState.isSuccess();
      }
    } finally {
      if (success) {
        rsp.add(STATUS, STATUS_SUCCESS);
      } else {
        rsp.add(STATUS, STATUS_FAILURE);
      }
      if (collectionState != null) {
        rsp.add(ENCRYPTION_STATE, collectionState.value);
      }
      if (log.isInfoEnabled()) {
        long timeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs);
        log.info("Responding encryption distributed state={} success={} for keyId={} collection={} timeMs={}",
            (collectionState == null ? null : collectionState.value), success, keyId, collectionName, timeMs);
      }
    }
  }

  private SolrClientHolder getHttpSolrClient(SolrQueryRequest req) {
    CoreContainer coreContainer = req.getCore().getCoreContainer();
    CloudSolrClient cloudSolrClient = coreContainer.getSolrClientCache().getCloudSolrClient(coreContainer.getZkController().getZkClient().getZkServerAddress());
    if (cloudSolrClient instanceof CloudHttp2SolrClient) {
      return new SolrClientHolder(((CloudHttp2SolrClient) cloudSolrClient).getHttpClient(), false);
    }
    return new SolrClientHolder(new Http2SolrClient.Builder().build(), true);
  }

  protected ModifiableSolrParams createDistributedRequestParams(SolrQueryRequest req, SolrQueryResponse rsp, String keyId) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(PARAM_KEY_ID, keyId == null ? NO_KEY_ID : keyId);
    return params;
  }

  boolean isTimeout(long maxTimeNs) {
    return System.nanoTime() > maxTimeNs;
  }

  private State sendEncryptionRequestWithRetry(Replica replica, ModifiableSolrParams params, Http2SolrClient httpSolrClient, String keyId, String collection) {
    for (int numAttempts = 0; numAttempts < DISTRIBUTION_MAX_ATTEMPTS; numAttempts++) {
      try {
        NamedList<Object> response = sendEncryptionRequest(replica, params, httpSolrClient);
        Object responseStatus = response.get(STATUS);
        Object responseState = response.get(ENCRYPTION_STATE);
        log.info("Encryption state {} status {} for replica {} keyId {} collection={}", responseStatus, responseState, replica.getName(), keyId, collection);
        if (responseState != null) {
          return State.fromValue(responseState.toString());
        }
      } catch (SolrServerException | IOException e) {
        log.error("Error occurred while sending encryption request", e);
      }
    }
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed encryption request to replica " + replica.getName() + " for keyId " + keyId + " collection " + collection);
  }

  private NamedList<Object> sendEncryptionRequest(Replica replica, ModifiableSolrParams params, Http2SolrClient httpSolrClient)
      throws SolrServerException, IOException {
    GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.POST, getPath(), params);
    request.setBasePath(replica.getCoreUrl());
    return httpSolrClient.request(request);
  }

  protected String getPath() {
    return "/admin/encrypt";
  }

  private void commitEmptyIndexForEncryption(@Nullable String keyId,
                                             SegmentInfos segmentInfos,
                                             SolrQueryRequest req,
                                             SolrQueryResponse rsp) throws IOException {
    // Commit no change, with the new active key id in the commit user data.
    log.debug("Commit on empty index for keyId={}", keyId);
    CommitUpdateCommand commitCmd = new CommitUpdateCommand(req, false);
    commitCmd.commitData = new HashMap<>(segmentInfos.getUserData());
    commitCmd.commitData.remove(COMMIT_ENCRYPTION_PENDING);
    setNewActiveKeyIdInCommit(keyId, commitCmd, rsp);
    assert !commitCmd.commitData.isEmpty();
    req.getCore().getUpdateHandler().commit(commitCmd);
  }

  private void setNewActiveKeyIdInCommit(String keyId,
                                         CommitUpdateCommand commitCmd,
                                         SolrQueryResponse rsp) throws IOException {
    if (keyId == null) {
      removeActiveKeyRefFromCommit(commitCmd.commitData);
      ensureNonEmptyCommitDataForEmptyCommit(commitCmd.commitData);
    } else {
      Map<String, String> keyCookie = buildKeyCookie(keyId, commitCmd.getReq(), rsp);
      EncryptionUtil.setNewActiveKeyIdInCommit(keyId, keyCookie, commitCmd.commitData);
    }
  }

  private void commitEncryptionComplete(String keyId,
                                        SegmentInfos segmentInfos,
                                        SolrQueryRequest req) throws IOException {
    assert isCommitActiveKeyId(keyId, segmentInfos);
    log.debug("Commit encryption complete for keyId={}", keyId);
    CommitUpdateCommand commitCmd = new CommitUpdateCommand(req, false);
    commitCmd.commitData = new HashMap<>(segmentInfos.getUserData());
    commitCmd.commitData.remove(COMMIT_ENCRYPTION_PENDING);
    // All segments are encrypted with the key id,
    // clear the oldest inactive key ids from the commit user data.
    clearOldInactiveKeyIdsFromCommit(commitCmd.commitData);
    ensureNonEmptyCommitDataForEmptyCommit(commitCmd.commitData);
    req.getCore().getUpdateHandler().commit(commitCmd);
  }

  private void ensureNonEmptyCommitDataForEmptyCommit(Map<String, String> commitData) {
    if (commitData.isEmpty()) {
      // Ensure that there is some data in the commit user data so that an empty commit
      // (with no change) is allowed.
      commitData.put("crypto.cleartext", "true");
    }
  }

  private void commitEncryptionStart(String keyId,
                                     SegmentInfos segmentInfos,
                                     SolrQueryRequest req,
                                     SolrQueryResponse rsp) throws IOException {
    log.debug("Commit encryption starting for keyId={}", keyId);
    CommitUpdateCommand commitCmd = new CommitUpdateCommand(req, false);
    commitCmd.commitData = new HashMap<>(segmentInfos.getUserData());
    commitCmd.commitData.put(COMMIT_ENCRYPTION_PENDING, "true");
    setNewActiveKeyIdInCommit(keyId, commitCmd, rsp);
    req.getCore().getUpdateHandler().commit(commitCmd);
  }

  private void encryptAsync(SolrQueryRequest req, long startTimeNs) {
    log.debug("Submitting async encryption");
    executor.submit(() -> {
      try {
        EncryptionUpdateLog updateLog = getEncryptionUpdateLog(req.getCore());
        if (updateLog != null) {
          log.debug("Encrypting update log");
          boolean logEncryptionComplete = updateLog.encryptLogs();
          log.info("{} encrypted the update log in {}",
                  logEncryptionComplete ? "Successfully" : "Partially", elapsedTime(startTimeNs));
          // If the logs encryption is not complete, it means some logs are currently in use.
          // The encryption will be automatically be retried after the next commit which should
          // release the old transaction log and make it ready for encryption.
        }

        log.debug("Triggering index encryption");
        CommitUpdateCommand commitCmd = new CommitUpdateCommand(req, true);
        // Trigger EncryptionMergePolicy.findForcedMerges() to re-encrypt
        // each segment which is not encrypted with the latest active key id.
        commitCmd.maxOptimizeSegments = Integer.MAX_VALUE;
        req.getCore().getUpdateHandler().commit(commitCmd);
        log.info("Successfully triggered index encryption with commit in {}", elapsedTime(startTimeNs));

      } catch (IOException e) {
        log.error("Exception while encrypting the index after {}", elapsedTime(startTimeNs), e);
      } finally {
        synchronized (pendingEncryptionLock) {
          pendingEncryptions.remove(req.getCore().getName());
        }
      }
      return null;
    });
  }

  private boolean areAllSegmentsEncryptedWithKeyId(@Nullable String keyId,
                                                   SolrCore core,
                                                   SegmentInfos segmentInfos) throws IOException {
    DirectoryFactory directoryFactory = core.getDirectoryFactory();
    Directory indexDir = directoryFactory.get(core.getIndexDir(),
                                              DirectoryFactory.DirContext.DEFAULT,
                                              DirectoryFactory.LOCK_TYPE_NONE);
    try {
      EncryptionDirectory dir = (EncryptionDirectory) indexDir;
      List<SegmentCommitInfo> segmentsWithOldKeyId = dir.getSegmentsWithOldKeyId(segmentInfos, keyId);
      log.debug("Encryption is pending; {} segments do not have keyId={}",
                segmentsWithOldKeyId.size(), keyId);
      return segmentsWithOldKeyId.isEmpty();
    } finally {
      directoryFactory.release(indexDir);
    }
  }

  private boolean areAllLogsEncryptedWithKeyId(String keyId, SolrCore core, SegmentInfos segmentInfos)
    throws IOException {
    EncryptionUpdateLog updateLog = getEncryptionUpdateLog(core);
    return updateLog == null || updateLog.areAllLogsEncryptedWithKeyId(keyId, segmentInfos.getUserData());
  }

  private EncryptionUpdateLog getEncryptionUpdateLog(SolrCore core) {
    UpdateHandler updateHandler = core.getUpdateHandler();
    if (updateHandler == null) {
      return null;
    }
    if (!(updateHandler.getUpdateLog() instanceof EncryptionUpdateLog)) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                              UpdateLog.class.getSimpleName()
                                + " must be configured with an "
                                + EncryptionUpdateLog.class.getSimpleName());
    }
    return (EncryptionUpdateLog) updateHandler.getUpdateLog();
  }

  private boolean isCommitActiveKeyId(String keyId, SegmentInfos segmentInfos) {
    String keyRef = getActiveKeyRefFromCommit(segmentInfos.getUserData());
    String activeKeyId = keyRef == null ? null : getKeyIdFromCommit(keyRef, segmentInfos.getUserData());
    return Objects.equals(keyId, activeKeyId);
  }

  private static String elapsedTime(long startTimeNs) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs) + " ms";
  }

  /**
   * Wraps a nullable key id (null key id means cleartext).
   */
  private static class PendingKeyId {
    @Nullable
    final String keyId;

    PendingKeyId(@Nullable String keyId) {
      this.keyId = keyId;
    }
  }

  private static class SolrClientHolder implements AutoCloseable {

    final Http2SolrClient httpSolrClient;
    final boolean shouldClose;

    SolrClientHolder(Http2SolrClient httpSolrClient, boolean shouldClose) {
      this.httpSolrClient = httpSolrClient;
      this.shouldClose = shouldClose;
    }

    Http2SolrClient getClient() {
      return httpSolrClient;
    }

    @Override
    public void close() {
      if (shouldClose) {
        httpSolrClient.close();
      }
    }
  }
}
