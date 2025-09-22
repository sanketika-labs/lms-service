package org.sunbird.learner.actors.activity.dao;

import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.RequestContext;
import org.sunbird.models.activity.ActivityBatch;

/**
 * DAO interface for ActivityBatch operations.
 * Handles CRUD operations for activity batches across different activity types (CF, CL, etc.)
 */
public interface ActivityBatchDao {

    /**
     * Create activity batch.
     *
     * @param requestContext Request context
     * @param activityBatch Activity batch information to be created
     * @return Response containing identifier of created activity batch
     */
    Response create(RequestContext requestContext, ActivityBatch activityBatch);

    /**
     * Update activity batch.
     *
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @param batchId Batch identifier
     * @param activityBatchMap Activity batch information to be updated
     * @return Response containing status of activity batch update
     */
    Response update(RequestContext requestContext, String activityId, String batchId, Map<String, Object> activityBatchMap);

    /**
     * Read activity batch for given identifier.
     *
     * @param activityId Activity identifier
     * @param batchId Batch identifier
     * @param requestContext Request context
     * @return Activity batch information
     */
    ActivityBatch readById(String activityId, String batchId, RequestContext requestContext);

    /**
     * Get activity batch as Map.
     *
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @param batchId Batch identifier
     * @return Activity batch information as Map
     */
    Map<String, Object> getActivityBatch(RequestContext requestContext, String activityId, String batchId);

    /**
     * List activity batches for a given activityId.
     *
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @return List of activity batches
     */
    java.util.List<java.util.Map<String, Object>> listByActivityId(RequestContext requestContext, String activityId);

    /**
     * Delete specified activity batch.
     *
     * @param requestContext Request context
     * @param batchId Activity batch identifier
     * @return Response containing status of activity batch delete
     */
    Response delete(RequestContext requestContext, String batchId);

    /**
     * Attaches a certificate template to activity batch
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @param batchId Batch identifier
     * @param templateId Template identifier
     * @param templateDetails Template details
     */
    void addCertificateTemplateToActivityBatch(
            RequestContext requestContext, String activityId, String batchId, String templateId, Map<String, Object> templateDetails);

    /**
     * Removes an attached certificate template from activity batch
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @param batchId Batch identifier
     * @param templateId Template identifier
     */
    void removeCertificateTemplateFromActivityBatch(RequestContext requestContext, String activityId, String batchId, String templateId);
}
