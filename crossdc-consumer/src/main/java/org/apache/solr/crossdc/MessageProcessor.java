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
package org.apache.solr.crossdc;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.crossdc.common.CrossDcConstants;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.SolrExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Message processor implements all the logic to process a MirroredSolrRequest.
 * It handles:
 *  1. Sending the update request to Solr
 *  2. Discarding or retrying failed requests
 *  3. Flagging requests for resubmission by the underlying consumer implementation.
 */
public class MessageProcessor  implements IQueueHandler<MirroredSolrRequest>  {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    final CloudSolrClient client;
    private final CrossDcConsumer.ResubmitBackoffPolicy resubmitBackoffPolicy;
    private static final String VERSION_FIELD = "_version_";

    public MessageProcessor(CloudSolrClient client, CrossDcConsumer.ResubmitBackoffPolicy resubmitBackoffPolicy) {
        this.client = client;
        this.resubmitBackoffPolicy = resubmitBackoffPolicy;
    }

    @Override
    public Result<MirroredSolrRequest> handleItem(MirroredSolrRequest mirroredSolrRequest) {
        connectToSolrIfNeeded();
        preventCircularMirroring(mirroredSolrRequest);
        return processMirroredRequest(mirroredSolrRequest);
    }

    private Result<MirroredSolrRequest> processMirroredRequest(MirroredSolrRequest request) {
        final Result<MirroredSolrRequest> result = handleSolrRequest(request);
        // Back-off before returning
        backoffIfNeeded(result);
        return result;
    }

    private Result<MirroredSolrRequest> handleSolrRequest(MirroredSolrRequest mirroredSolrRequest) {
        logger.debug("Handling Solr request");
        SolrRequest request = mirroredSolrRequest.getSolrRequest();
        final SolrParams requestParams = request.getParams();

        final String shouldMirror = requestParams.get("shouldMirror");
        if (shouldMirror != null && !Boolean.parseBoolean(shouldMirror)) {
            logger.warn("Skipping mirrored request because shouldMirror is set to false. request={}", request);
            return new Result<>(ResultStatus.FAILED_NO_RETRY);
        }
        logFirstAttemptLatency(mirroredSolrRequest);

        Result<MirroredSolrRequest> result;
        try {
            prepareIfUpdateRequest(request);
            logRequest(request);
            logger.debug("About to submit Solr request {}", request);
            result = processMirroredSolrRequest(request);
        } catch (Exception e) {
            result = handleException(mirroredSolrRequest, e);
        }

        return result;
    }

    private Result<MirroredSolrRequest> handleException(MirroredSolrRequest mirroredSolrRequest, Exception e) {
        final SolrException solrException = SolrExceptionUtil.asSolrException(e);
        logIf4xxException(solrException);
        if (!isRetryable(e)) {
            logFailure(mirroredSolrRequest, e, solrException, false);
            return new Result<>(ResultStatus.FAILED_NO_RETRY, e);
        } else {
            logFailure(mirroredSolrRequest, e, solrException, true);
            mirroredSolrRequest.setAttempt(mirroredSolrRequest.getAttempt() + 1);
            maybeBackoff(solrException);
            return new Result<>(ResultStatus.FAILED_RESUBMIT, e, mirroredSolrRequest);
        }
    }

    private void maybeBackoff(SolrException solrException) {
        if (solrException == null) {
            return;
        }
        long sleepTimeMs = 1000;
        String backoffTimeSuggested = solrException.getMetadata("backoffTime-ms");
        if (backoffTimeSuggested != null && !"0".equals(backoffTimeSuggested)) {
            // If backoff policy is not configured (returns "0" by default), then sleep 1 second. If configured, do as it says.
            sleepTimeMs = Math.max(1, Long.parseLong(backoffTimeSuggested));
        }
        logger.info("Consumer backoff. sleepTimeMs={}", sleepTimeMs);
        uncheckedSleep(sleepTimeMs);
    }

    private boolean isRetryable(Exception e) {
        SolrException se = SolrExceptionUtil.asSolrException(e);

        if (se != null) {
            int code = se.code();
            if (code == SolrException.ErrorCode.CONFLICT.code) {
                return false;
            }
        }
        // Everything other than version conflict exceptions should be retried.
        logger.warn("Unexpected exception, will resubmit the request to the queue", e);
        return true;
    }

    private void logIf4xxException(SolrException solrException) {
        // This shouldn't really happen but if it doesn, it most likely requires fixing in the return code from Solr.
        if (solrException != null && 400 <= solrException.code() && solrException.code() < 500) {
            logger.error("Exception occurred with 4xx response. {}", solrException);
        }
    }

    private void logFailure(MirroredSolrRequest mirroredSolrRequest, Exception e, SolrException solrException, boolean retryable) {
        // This shouldn't really happen.
        if (solrException != null && 400 <= solrException.code() && solrException.code() < 500) {
            logger.error("Exception occurred with 4xx response. {}", solrException);
            return;
        }

        final StringBuffer msg = new StringBuffer();
        msg.append("errorCode=").append(solrException != null ? solrException.code() : -1);
        msg.append(" retryCount=").append(mirroredSolrRequest.getAttempt());
        logger.warn("Resubmitting mirrored solr request after failure {} exception={}", msg, e);
    }

    /**
     *
     * Process the SolrRequest. If not, this method throws an exception.
     */
    private Result<MirroredSolrRequest> processMirroredSolrRequest(SolrRequest request) throws Exception {
        Result<MirroredSolrRequest> result;
        SolrResponseBase response = (SolrResponseBase) request.process(client);

        int status = response.getStatus();
        if (status != 0) {
            throw new SolrException(SolrException.ErrorCode.getErrorCode(status), "response=" + response);
        }

        result = new Result<>(ResultStatus.HANDLED);
        return result;
    }

    private void logRequest(SolrRequest request) {
        if(request instanceof UpdateRequest) {
            final StringBuffer rmsg = new StringBuffer("Submitting update request");
            if(((UpdateRequest) request).getDeleteById() != null) {
                rmsg.append(" numDeleteByIds=").append(((UpdateRequest) request).getDeleteById().size());
            }
            if(((UpdateRequest) request).getDocuments() != null) {
                rmsg.append(" numUpdates=").append(((UpdateRequest) request).getDocuments().size());
            }
            if(((UpdateRequest) request).getDeleteQuery() != null) {
                rmsg.append(" numDeleteByQuery=").append(((UpdateRequest) request).getDeleteQuery().size());
            }
            logger.info(rmsg.toString());
        }
    }

    /**
     * Clean up the Solr request to be submitted locally.
     * @param request The SolrRequest to be cleaned up for submitting locally.
     */
    private void prepareIfUpdateRequest(SolrRequest request) {
        if (request instanceof UpdateRequest) {
            // Remove versions from add requests
            UpdateRequest updateRequest = (UpdateRequest) request;

            List<SolrInputDocument> documents = updateRequest.getDocuments();
            if (documents != null) {
                for (SolrInputDocument doc : documents) {
                    sanitizeDocument(doc);
                }
            }
            removeVersionFromDeleteByIds(updateRequest);
        }
    }

    /**
     * Strips fields that are problematic for replication.
     */
    private void sanitizeDocument(SolrInputDocument doc) {
        doc.remove(VERSION_FIELD);

        // TODO: This could optionally strip more fields if configured
        /**
         for (String fieldToStrip : CrossDcProps.SolrInputDocumentFieldsToStrip.getValue()) {
         doc.removeField(fieldToStrip);
         }
         **/
    }

    private void removeVersionFromDeleteByIds(UpdateRequest updateRequest) {
        Map<String, Map<String, Object>> deleteIds = updateRequest.getDeleteByIdMap();
        if (deleteIds != null) {
            for (Map<String, Object> idParams : deleteIds.values()) {
                if (idParams != null) {
                    idParams.put(UpdateRequest.VER, null);
                }
            }
        }
    }

    private void logFirstAttemptLatency(MirroredSolrRequest mirroredSolrRequest) {
        // Only record the latency of the first attempt, essentially measuring the latency from submitting on the
        // primary side until the request is eligible to be consumed on the buddy side (or vice versa).
        if (mirroredSolrRequest.getAttempt() == 1) {
            logger.debug("First attempt latency = {}",
                    System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(mirroredSolrRequest.getSubmitTimeNanos()));
        }
    }

    /**
     * Adds {@link CrossDcConstants#SHOULD_MIRROR}=false to the params if it's not already specified.
     * Logs a warning if it is specified and NOT set to false. (i.e. circular mirror may occur)
     *
     * @param mirroredSolrRequest MirroredSolrRequest object that is being processed.
     */
    private void preventCircularMirroring(MirroredSolrRequest mirroredSolrRequest) {
        if (mirroredSolrRequest.getSolrRequest() instanceof UpdateRequest) {
            UpdateRequest updateRequest = (UpdateRequest) mirroredSolrRequest.getSolrRequest();
            ModifiableSolrParams params = updateRequest.getParams();
            String shouldMirror = (params == null ? null : params.get(CrossDcConstants.SHOULD_MIRROR));
            if (shouldMirror == null) {
                logger.warn(CrossDcConstants.SHOULD_MIRROR + " param is missing - setting to false. Request={}", mirroredSolrRequest);
                updateRequest.setParam(CrossDcConstants.SHOULD_MIRROR, "false");
            } else if (!"false".equalsIgnoreCase(shouldMirror)) {
                logger.warn(CrossDcConstants.SHOULD_MIRROR + " param equal to " + shouldMirror);
            }
        } else {
            SolrParams params = mirroredSolrRequest.getSolrRequest().getParams();
            String shouldMirror = (params == null ? null : params.get(CrossDcConstants.SHOULD_MIRROR));
            if (shouldMirror == null) {
                if (params instanceof ModifiableSolrParams) {
                    logger.warn(CrossDcConstants.SHOULD_MIRROR + " param is missing - setting to false");
                    ((ModifiableSolrParams) params).set(CrossDcConstants.SHOULD_MIRROR, "false");
                } else {
                    logger.warn(CrossDcConstants.SHOULD_MIRROR + " param is missing and params are not modifiable");
                }
            } else if (!"false".equalsIgnoreCase(shouldMirror)) {
                logger.warn(CrossDcConstants.SHOULD_MIRROR + " param is present and set to " + shouldMirror);
            }
        }
    }

    private void connectToSolrIfNeeded() {
        // Don't try to consume anything if we can't connect to the solr server
        boolean connected = false;
        while (!connected) {
            try {
                client.connect(); // volatile null-check if already connected
                connected = true;
            } catch (Exception e) {
                logger.error("Unable to connect to solr server. Not consuming.", e);
                uncheckedSleep(5000);
            }
        }
    }

    void uncheckedSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void backoffIfNeeded(Result<MirroredSolrRequest> result) {
        if (result.status().equals(ResultStatus.FAILED_RESUBMIT)) {
            final long backoffMs = resubmitBackoffPolicy.getBackoffTimeMs(result.newItem());
            if (backoffMs > 0L) {
                try {
                    Thread.sleep(backoffMs);
                } catch (final InterruptedException ex) {
                    // we're about to exit the method anyway, so just log this and return the item. Let the caller
                    // handle it.
                    logger.warn("Thread interrupted while backing off before retry");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
