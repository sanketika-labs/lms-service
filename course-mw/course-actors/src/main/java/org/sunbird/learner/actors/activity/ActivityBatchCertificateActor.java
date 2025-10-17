package org.sunbird.learner.actors.activity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.learner.actors.coursebatch.BaseBatchMgmtActor;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.learner.actors.activity.dao.ActivityBatchDao;
import org.sunbird.learner.actors.activity.impl.ActivityBatchDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.activity.ActivityBatch;

import java.util.HashMap;
import java.util.Map;

/**
 * Actor for managing certificate templates for activity batches.
 * Handles operations like adding certificate templates to activity batches.
 */
public class ActivityBatchCertificateActor extends BaseBatchMgmtActor {

    private ActivityBatchDao activityBatchDao = new ActivityBatchDaoImpl();
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Handles incoming requests for activity batch certificate operations.
     * Dispatches the request to the appropriate handler based on the operation type.
     *
     * @param request the incoming request object
     * @throws Throwable if any error occurs during processing
     */
    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass().getName());

        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "addCertificateToActivityBatch":
                addCertificateToActivityBatch(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    /**
     * Adds a certificate template to an activity batch.
     * Validates the request, retrieves the existing batch, updates it with certificate template,
     * and persists the changes.
     *
     * @param actorMessage the request object containing certificate template details
     * @throws Exception if any error occurs during the operation
     */
    private void addCertificateToActivityBatch(Request actorMessage) throws Exception {
        Map<String, Object> request = actorMessage.getRequest();
        RequestContext requestContext = actorMessage.getRequestContext();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);

        // Extract batch information from request
        Map<String, Object> batchInfo = (Map<String, Object>) request.get(JsonKey.BATCH);
        String activityId = (String) batchInfo.get(JsonKey.ACTIVITYID);
        String batchId = (String) batchInfo.get(JsonKey.BATCH_ID);
        Map<String, Object> template = (Map<String, Object>) batchInfo.get("template");

        logger.info(requestContext, 
                "ActivityBatchCertificateActor:addCertificateToActivityBatch - Processing request for activityId=" 
                + activityId + ", batchId=" + batchId + ", requestedBy=" + requestedBy);

        // Retrieve existing batch
        ActivityBatch existingBatch = activityBatchDao.readById(activityId, batchId, requestContext);
        
        if (existingBatch == null) {
            String errorMsg = "ActivityBatchCertificateActor:addCertificateToActivityBatch - Batch not found for activityId=" 
                    + activityId + ", batchId=" + batchId;
            RuntimeException ex = new RuntimeException("Activity batch not found");
            logger.error(requestContext, errorMsg, ex);
            throw ex;
        }

        logger.info(requestContext, 
                "ActivityBatchCertificateActor:addCertificateToActivityBatch - Found existing batch for activityId=" 
                + activityId + ", batchId=" + batchId);

        // Add certificate template to the batch with proper JSON serialization
        String templateId = (String) template.get(JsonKey.IDENTIFIER);
        
        // Add url field if previewUrl exists (to match course batch behavior)
        if (template.containsKey("previewUrl") && !template.containsKey(JsonKey.URL)) {
            template.put(JsonKey.URL, template.get("previewUrl"));
        }
        
        try {
            // Convert nested objects to JSON strings to satisfy frozen<map<text, text>> constraint
            Map<String, String> serializedTemplate = new HashMap<>();
            for (Map.Entry<String, Object> entry : template.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                
                if (value instanceof Map || value instanceof java.util.List) {
                    // Serialize complex objects to JSON strings
                    serializedTemplate.put(key, mapper.writeValueAsString(value));
                } else {
                    // Keep simple values as strings
                    serializedTemplate.put(key, value != null ? value.toString() : null);
                }
            }
            
            // Use DAO method with serialized template (cast to Map<String, Object>)
            Map<String, Object> templateForDao = new HashMap<>(serializedTemplate);
            activityBatchDao.addCertificateTemplateToActivityBatch(requestContext, activityId, batchId, templateId, templateForDao);
            
        } catch (Exception e) {
            logger.error(requestContext, "ActivityBatchCertificateActor:addCertificateToActivityBatch - Failed to serialize template", e);
            throw new RuntimeException("Failed to serialize certificate template", e);
        }
        
        // Create response
        Response result = new Response();
        result.put(JsonKey.RESPONSE, JsonKey.SUCCESS);

        logger.info(requestContext, 
                "ActivityBatchCertificateActor:addCertificateToActivityBatch - Successfully added certificate template to batch. activityId=" 
                + activityId + ", batchId=" + batchId);

        // Send success response
        result.put(JsonKey.BATCH_ID, batchId);
        result.put(JsonKey.ACTIVITYID, activityId);
        sender().tell(result, self());
    }
}