package org.apache.solr.encryption.kms;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.encryption.EncryptionRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.Map;

/**
 * Extension of {@link EncryptionRequestHandler} that gets required parameters
 * {@link #PARAM_TENANT_ID} and {@link #PARAM_ENCRYPTION_KEY_BLOB} from the
 * {@link SolrQueryRequest} to pass later to the {@link KmsKeySupplier}.
 */
public class KmsEncryptionRequestHandler extends EncryptionRequestHandler {

  /**
   * Tenant Id request parameter - required.
   */
  public static final String PARAM_TENANT_ID = "tenantId";
  /**
   * Data Key Blob request parameter - required.
   */
  public static final String PARAM_ENCRYPTION_KEY_BLOB = "encryptionKeyBlob";

  /**
   * Builds the KMS key cookie based on key id and key blob parameters of the request.
   * If a required parameter is missing, this method throws a {@link SolrException} with
   * {@link SolrException.ErrorCode#BAD_REQUEST} and sets the response status to failure.
   */
  @Override
  protected Map<String, String> buildKeyCookie(String keyId,
                                               SolrQueryRequest req,
                                               SolrQueryResponse rsp) {
    String tenantId = getRequiredRequestParam(req, PARAM_TENANT_ID, rsp);
    String encryptionKeyBlob = getRequiredRequestParam(req, PARAM_ENCRYPTION_KEY_BLOB, rsp);
    return Map.of(
        PARAM_TENANT_ID, tenantId,
        PARAM_ENCRYPTION_KEY_BLOB, encryptionKeyBlob
    );
  }

  private String getRequiredRequestParam(SolrQueryRequest req, String param, SolrQueryResponse rsp) {
    String arg = req.getParams().get(param);
    if (arg == null || arg.isEmpty()) {
      rsp.add(STATUS, STATUS_FAILURE);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required parameter " + param + " must be present and not empty.");
    }
    return arg;
  }

  @Override
  protected ModifiableSolrParams createDistributedRequestParams(SolrQueryRequest req, SolrQueryResponse rsp, String keyId) {
    return super.createDistributedRequestParams(req, rsp, keyId)
        .set(PARAM_TENANT_ID, getRequiredRequestParam(req, PARAM_TENANT_ID, rsp))
        .set(PARAM_ENCRYPTION_KEY_BLOB, getRequiredRequestParam(req, PARAM_ENCRYPTION_KEY_BLOB, rsp));
  }
}
