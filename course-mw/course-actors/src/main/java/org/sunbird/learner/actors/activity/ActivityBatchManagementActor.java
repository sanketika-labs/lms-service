package org.sunbird.learner.actors.activity;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.learner.actors.coursebatch.BaseBatchMgmtActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.util.JsonUtil;
import org.sunbird.userorg.UserOrgService;
import org.sunbird.userorg.UserOrgServiceImpl;
import org.sunbird.kafka.client.InstructionEventGenerator;
import org.sunbird.learner.actors.activity.dao.ActivityBatchDao;
import org.sunbird.learner.actors.activity.impl.ActivityBatchDaoImpl;
import org.sunbird.learner.util.ActivityBatchUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.activity.ActivityBatch;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ActivityBatchManagementActor extends BaseBatchMgmtActor {

    private ActivityBatchDao activityBatchDao = new ActivityBatchDaoImpl();
    private UserOrgService userOrgService = UserOrgServiceImpl.getInstance();
    private String dateFormat = "yyyy-MM-dd";
    private String timeZone = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE);

    /**
     * Handles incoming requests for activity batch management operations such as create, update, and list.
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
            case "createBatch":
                createActivityBatch(request, false);
                break;
            case "privateCreateBatch":
                createActivityBatch(request, true);
                break;
            case "updateBatch":
                updateActivityBatch(request);
                break;
            case "getActivityBatches":
                listActivityBatches(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    /**
     * Composes a batch ID for the activity batch. If fromReq is true, uses the provided batch ID from the request;
     * otherwise, generates a new unique batch ID based on the environment.
     *
     * @param request the request object
     * @param fromReq flag indicating whether to use the batch ID from the request
     * @return the composed or generated batch ID
     */
    private String composeBatchId(Request request, boolean fromReq) {
        String provided = fromReq ? (String) request.get(JsonKey.BATCH_ID) : null;
        if (StringUtils.isBlank(provided)) {
            return ProjectUtil.getUniqueIdFromTimestamp(request.getEnv());
        }
        return provided;
    }

    /**
     * Lists all activity batches for a given activity ID. Validates the activity ID and fetches the batch list from the DAO.
     * Sends the result back to the sender.
     *
     * @param actorMessage the request object containing context and activity ID
     */
    private void listActivityBatches(Request actorMessage) {
        String activityId = (String) actorMessage.getContext().get(JsonKey.ACTIVITYID);
        RequestContext requestContext = actorMessage.getRequestContext();
        logger.info(requestContext, "ActivityBatchManagementActor:listActivityBatches: activityId=" + activityId);
        //getCollectionDetails(requestContext, activityId);
        java.util.List<java.util.Map<String, Object>> result = activityBatchDao.listByActivityId(requestContext, activityId);
        logger.info(requestContext, "ActivityBatchManagementActor:listActivityBatches: Found " + (result != null ? result.size() : 0) + " batches for activityId=" + activityId);
        Response response = new Response();
        response.put(JsonKey.RESPONSE, result);
        sender().tell(response, self());
    }

    /**
     * Creates a new activity batch. Validates input, sets batch status, persists the batch, updates collection metadata,
     * and generates an event if the activity type is primary.
     *
     * @param actorMessage the request object containing batch creation details
     * @param idFromRequest flag indicating whether to use the batch ID from the request
     * @throws Exception if any error occurs during batch creation
     */
    private void createActivityBatch(Request actorMessage, boolean idFromRequest) throws Exception {
        Map<String, Object> request = actorMessage.getRequest();
        RequestContext requestContext = actorMessage.getRequestContext();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String batchId = composeBatchId(actorMessage, idFromRequest);
        String activityId = (String) request.get(JsonKey.ACTIVITYID);
        String activityType = (String) request.get(JsonKey.ACTIVITYTYPE);
        @SuppressWarnings({"unchecked", "unused"})
        Map<String, String> headers = (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);

        logger.info(requestContext, "ActivityBatchManagementActor:createActivityBatch - Incoming request: activityId=" + activityId + ", activityType=" + activityType + ", requestedBy=" + requestedBy + ", batchId=" + batchId);
        Map<String, Object> contentDetails = new HashMap<>();
        if(isPrimaryActivityType(activityType)) {
            contentDetails = validateActivityIdAndType(requestContext, activityId, activityType);
            logger.info(requestContext, "ActivityBatchManagementActor:createActivityBatch - Content details validated for activityId=" + activityId + ", activityType=" + activityType + ", contentDetails=" + contentDetails);
        }
        ActivityBatch activityBatch = JsonUtil.convert(request, ActivityBatch.class);
        activityBatch.setStatus(setActivityBatchStatus((String) request.get(JsonKey.START_DATE)));
        activityBatch.setCreatedDate(ProjectUtil.getTimeStamp());
        if (StringUtils.isBlank(activityBatch.getCreatedBy())) {
            activityBatch.setCreatedBy(requestedBy);
        }
        activityBatch.setBatchId(batchId);
        logger.info(requestContext, "ActivityBatchManagementActor:createActivityBatch - Validating createdFor organizations: " + activityBatch.getCreatedFor());
        validateContentOrg(requestContext, activityBatch.getCreatedFor());
        Response result = activityBatchDao.create(requestContext, activityBatch);

        // Sync to Elasticsearch
        try {
            Map<String, Object> esActivityBatchMap = ActivityBatchUtil.esActivityBatchMapping(activityBatch, dateFormat);
            ActivityBatchUtil.syncActivityBatchForeground(requestContext, batchId, esActivityBatchMap);
            logger.info(requestContext, "ActivityBatchManagementActor:createActivityBatch - Successfully synced to Elasticsearch for batchId=" + batchId);
        } catch (Exception e) {
            logger.error(requestContext, "ActivityBatchManagementActor:createActivityBatch - Failed to sync to Elasticsearch for batchId=" + batchId + ", error=" + e.getMessage(), e);
        }

        Map<String, Object> esActivityBatchMap = createActivityBatchMapping(activityBatch, dateFormat);

        if(isPrimaryActivityType(activityType)) {
            updateCollection(requestContext, esActivityBatchMap, contentDetails);
        }
        if (isPrimaryActivityType(activityType)) {
            // Create event data structure matching the target event format
            Map<String, Object> eventData = createBatchEventData(
                    batchId,
                    activityId,
                    activityType,
                    requestedBy,
                    request,
                    "create"
            );
            // Push event to Kafka
            String topic = ProjectUtil.getConfigValue("kafka_activity_batch_topic");
            InstructionEventGenerator.pushInstructionEvent(batchId, topic, eventData);
            logger.info(requestContext,
                    "ActivityBatchManagementActor:createBatch - Event published for the activityType " + activityType + ". ActivityId: " + activityId + ", batchId: " + batchId);
        } else {
            logger.info(requestContext,
                    "ActivityBatchManagementActor:createBatch - No event generated for activityType: " + activityType + ". ActivityId: " + activityId + ", batchId: " + batchId);
        }

        result.put(JsonKey.BATCH_ID, batchId);
        sender().tell(result, self());

        logger.info(requestContext,
                "ActivityBatchManagementActor:createBatch - ActivityBatch created for activityId: " + activityId + ", batchId: " + batchId + ", activityType: " + activityType);

    }

    /**
     * Updates an existing activity batch. Validates input, checks permissions, updates the batch, updates collection metadata,
     * and generates an event if the activity type is primary.
     *
     * @param actorMessage the request object containing batch update details
     * @throws Exception if any error occurs during batch update
     */
    private void updateActivityBatch(Request actorMessage) throws Exception {

        Map<String, Object> request = actorMessage.getRequest();
        RequestContext requestContext = actorMessage.getRequestContext();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String batchId = (String) request.get(JsonKey.BATCH_ID);
        String activityId = (String) request.get(JsonKey.ACTIVITYID);
        String activityType = (String) request.get("activityType");
        @SuppressWarnings({"unchecked", "unused"})
        Map<String, String> headers = (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);

        logger.info(requestContext, "ActivityBatchManagementActor:updateActivityBatch - Incoming request: activityId=" + activityId + ", activityType=" + activityType + ", requestedBy=" + requestedBy + ", batchId=" + batchId);
        Map<String, Object> contentDetails = new HashMap<>();
        if(isPrimaryActivityType(activityType)) {
            contentDetails = validateActivityIdAndType(requestContext, activityId, activityType);
            logger.info(requestContext, "ActivityBatchManagementActor:updateActivityBatch - Content details validated for activityId=" + activityId + ", activityType=" + activityType + ", contentDetails=" + contentDetails);
        }
        ActivityBatch existingBatch = activityBatchDao.readById(activityId, batchId, requestContext);
        logger.info(requestContext, "ActivityBatchManagementActor:updateActivityBatch - Existing batch fetched for batchId=" + batchId + ", activityId=" + activityId);
        validateUserPermission(existingBatch, requestedBy);
        logger.info(requestContext, "ActivityBatchManagementActor:updateActivityBatch - User permission validated for requestedBy=" + requestedBy);

        ActivityBatch activityBatch = JsonUtil.convert(request, ActivityBatch.class);

        logger.info(requestContext, "ActivityBatchManagementActor:updateActivityBatch - Validating createdFor organizations: " + activityBatch.getCreatedFor());
        validateContentOrg(requestContext, activityBatch.getCreatedFor());
        activityBatch.setUpdatedDate(ProjectUtil.getTimeStamp());
        
        // Use proper ActivityBatch object mapping like create operation
        Map<String, Object> updateData = ActivityBatchUtil.cassandraActivityBatchMapping(activityBatch, dateFormat);
        updateData.remove(JsonKey.BATCH_ID);
        updateData.remove(JsonKey.ACTIVITYID);

        Response result = activityBatchDao.update(requestContext, activityId, batchId, updateData);

        // Sync to Elasticsearch
        try {
            ActivityBatch updatedBatch = activityBatchDao.readById(activityId, batchId, requestContext);
            Map<String, Object> esActivityBatchMap = ActivityBatchUtil.esActivityBatchMapping(updatedBatch, dateFormat);
            ActivityBatchUtil.syncActivityBatchForeground(requestContext, batchId, esActivityBatchMap);
            logger.info(requestContext, "ActivityBatchManagementActor:updateActivityBatch - Successfully synced to Elasticsearch for batchId=" + batchId);
        } catch (Exception e) {
            logger.error(requestContext, "ActivityBatchManagementActor:updateActivityBatch - Failed to sync to Elasticsearch for batchId=" + batchId + ", error=" + e.getMessage(), e);
        }

        ActivityBatch updatedBatch = activityBatchDao.readById(activityId, batchId, requestContext);
        Map<String, Object> esActivityBatchMap = createActivityBatchMapping(updatedBatch, dateFormat);
        if(isPrimaryActivityType(activityType)) {
            updateCollection(requestContext, esActivityBatchMap, contentDetails);
        }

        //Generating an event into Kafka for configured primary activity types
        if (isPrimaryActivityType(activityType)) {
            // Create event data for Kafka
            Map<String, Object> eventData = createBatchEventData(
                    batchId,
                    activityId,
                    activityType,
                    requestedBy,
                    request,
                    "update"
            );

            // Push event to Kafka
            String topic = ProjectUtil.getConfigValue("kafka_activity_batch_topic");
            InstructionEventGenerator.pushInstructionEvent(batchId, topic, eventData);

            logger.info(requestContext,
                    "ActivityBatchManagementActor:updateBatch - Event published for the activityType " + activityType + ". BatchId: " + batchId + ", activityId: " + activityId);
        } else {
            logger.info(requestContext,
                    "ActivityBatchManagementActor:updateBatch - No event generated for activityType: " + activityType + ". BatchId: " + batchId + ", activityId: " + activityId);
        }

        result.put(JsonKey.BATCH_ID, batchId);
        sender().tell(result, self());

        logger.info(requestContext,
                "ActivityBatchManagementActor: updateBatch - ActivityBatch updated for batchId: " + batchId + ", activityId: " + activityId + ", activityType: " + activityType);
    }

    /**
     * Determines the status of an activity batch based on the start date.
     * Returns STARTED if the start date is today, otherwise NOT_STARTED.
     *
     * @param startDate the start date of the batch
     * @return the status value (STARTED or NOT_STARTED)
     */
    private int setActivityBatchStatus(String startDate) {
        try {
            SimpleDateFormat dateFormatter = ProjectUtil.getDateFormatter(dateFormat);
            dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
            Date todayDate = dateFormatter.parse(dateFormatter.format(new Date()));
            Date requestedStartDate = dateFormatter.parse(startDate);
            logger.info(null, "ActivityBatchManagementActor:setActivityBatchStatus: todayDate="
                    + todayDate + ", requestedStartDate=" + requestedStartDate);
            if (todayDate.compareTo(requestedStartDate) == 0) {
                return ProjectUtil.ProgressStatus.STARTED.getValue();
            } else {
                return ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
            }
        } catch (ParseException e) {
            logger.error(null, "ActivityBatchManagementActor:setActivityBatchStatus: Exception occurred with error message = " + e.getMessage(), e);
        }
        return ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    }

    /**
     * Creates event data for batch creation or update to be sent to Kafka.
     *
     * @param batchId the batch ID
     * @param activityId the activity ID
     * @param activityType the activity type
     * @param requestedBy the user who requested the operation
     * @param requestData the request data map
     * @param action the action type (create or update)
     * @return a map containing the event data
     */
    private Map<String, Object> createBatchEventData(
            String batchId,
            String activityId,
            String activityType,
            String requestedBy,
            Map<String, Object> requestData,
            String action) {

        Map<String, Object> eventData = new HashMap<>();

        // Set event ID and timestamp
        eventData.put("eid", "BE_JOB_REQUEST");
        eventData.put("ets", System.currentTimeMillis());
        eventData.put("mid", "LP." + System.currentTimeMillis() + "." + java.util.UUID.randomUUID().toString());

        // Actor information
        Map<String, Object> actor = new HashMap<>();
        actor.put("id", "Batch Creation Flink Job");
        actor.put("type", "System");
        eventData.put("actor", actor);

        // Context information
        Map<String, Object> context = new HashMap<>();
        Map<String, Object> pdata = new HashMap<>();
        pdata.put("ver", "1.0");
        pdata.put("id", "org.sunbird.platform");
        context.put("pdata", pdata);

        Map<String, Object> cdata = new HashMap<>();
        cdata.put("id", java.util.UUID.randomUUID().toString().replace("-", ""));
        cdata.put("type", "Request");
        context.put("cdata", cdata);

        eventData.put("context", context);

        // Object information - map activity type to readable names
        Map<String, Object> object = new HashMap<>();
        object.put("id", activityId);
        String objectType = getObjectTypeFromActivityType(activityType);
        object.put("type", objectType);
        eventData.put("object", object);

        // Event data
        Map<String, Object> edata = new HashMap<>();
        edata.put("action", "batch-" + action.toLowerCase());
        edata.put("batchId", batchId);
        edata.put("activityId", activityId);
        edata.put("activityType", activityType);
        edata.put("iteration", 1);

        // Add additional request data if present
        String[] supportedFields = {"name", "description", "startDate", "endDate", "enrollmentType",
                "enrollmentEndDate", "status", "createdFor"};

        for (String field : supportedFields) {
            if (requestData.containsKey(field)) {
                edata.put(field, requestData.get(field));
            }
        }

        eventData.put("edata", edata);

        return eventData;
    }

    /**
     * Returns the object type string for a given activity type. Defaults to "Content" if activityType is null.
     *
     * @param activityType the activity type
     * @return the object type string
     */
    private String getObjectTypeFromActivityType(String activityType) {
        return (activityType == null) ? "Content" : activityType;        
    }

    /**
     * Checks if the given activity type is a primary activity type as per configuration.
     *
     * @param activityType the activity type to check
     * @return true if the activity type is primary, false otherwise
     */
    private boolean isPrimaryActivityType(String activityType) {
        String configured = ProjectUtil.getConfigValue(JsonKey.PRIMARY_ACTIVITY_TYPES);
        // Default to ["Competency Framework"] when not configured
        String[] types = (configured == null || configured.trim().isEmpty())
                ? new String[]{"Competency Framework"}
                : configured.split(",");
        if (activityType == null) {
            return false;
        }
        String atLower = activityType.trim().toLowerCase();
        for (String t : types) {
            if (t != null && atLower.equals(t.trim().toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Validates if the user has permission to update the given activity batch.
     * Throws an exception if the user is not authorized.
     *
     * @param activityBatch the activity batch to check
     * @param requestedBy the user requesting the update
     */
    private void validateUserPermission(ActivityBatch activityBatch, String requestedBy) {
        String canUpdateBatch = activityBatch.getCreatedBy();
        
        if (ProjectUtil.getConfigValue(JsonKey.AUTH_ENABLED) != null && 
            Boolean.parseBoolean(ProjectUtil.getConfigValue(JsonKey.AUTH_ENABLED)) && 
            (canUpdateBatch == null || !canUpdateBatch.equals(requestedBy))) {
            throw new ProjectCommonException(
                    ResponseCode.unAuthorized.getErrorCode(),
                    ResponseCode.unAuthorized.getErrorMessage(),
                    ResponseCode.UNAUTHORIZED.getResponseCode());
        }
    }

    /**
     * Validates that all organization IDs in the createdFor list are valid organizations.
     * Throws an exception if any organization is invalid.
     *
     * @param requestContext the request context
     * @param createdFor the list of organization IDs to validate
     */
    private void validateContentOrg(RequestContext requestContext, List<String> createdFor) {
        if (createdFor != null) {
            for (String orgId : createdFor) {
                if (!isOrgValid(requestContext, orgId)) {
                    throw new ProjectCommonException(
                            ResponseCode.invalidOrgId.getErrorCode(),
                            ResponseCode.invalidOrgId.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
            }
        }
    }

    /**
     * Checks if the given organization ID is valid by querying the user organization service.
     *
     * @param requestContext the request context
     * @param orgId the organization ID to check
     * @return true if the organization is valid, false otherwise
     */
    private boolean isOrgValid(RequestContext requestContext, String orgId) {
        try {
            Map<String, Object> result = userOrgService.getOrganisationById(orgId);
            logger.debug(requestContext, "ActivityBatchManagementActor:isOrgValid: orgId = "
                    + (MapUtils.isNotEmpty(result) ? result.get(JsonKey.ID) : null));
            return (MapUtils.isNotEmpty(result) && orgId.equals(result.get(JsonKey.ID)));
        } catch (Exception e) {
            logger.error(requestContext, "Error while fetching OrgID : " + orgId, e);
        }
        return false;
    }

    /**
     * Creates a mapping of ActivityBatch for elasticsearch/collection updates.
     * Similar to CourseBatchUtil.esCourseMapping but for ActivityBatch.
     *
     * @param activityBatch The ActivityBatch object to map
     * @param dateFormat The date format to use for date fields
     * @return Map containing the mapped activity batch data
     */
    private Map<String, Object> createActivityBatchMapping(ActivityBatch activityBatch, String dateFormat) {
        Map<String, Object> map = new HashMap<>();
        
        // Add all fields
        map.put(JsonKey.BATCH_ID, activityBatch.getBatchId());
        map.put(JsonKey.NAME, activityBatch.getName());
        map.put(JsonKey.COURSE_CREATED_FOR, activityBatch.getCreatedFor());
        map.put(JsonKey.START_DATE, activityBatch.getStartDate());
        map.put(JsonKey.END_DATE, activityBatch.getEndDate());
        map.put(JsonKey.ENROLLMENT_TYPE, activityBatch.getEnrollmentType());
        map.put(JsonKey.STATUS, activityBatch.getStatus());
        map.put(JsonKey.ENROLLMENT_END_DATE, activityBatch.getEnrollmentEndDate());
        map.put(JsonKey.COURSE_ID, activityBatch.getActivityId()); // Map activityId to courseId for consistency
        
        // Remove all null values
        map.entrySet().removeIf(entry -> entry.getValue() == null);
        
        return map;
    }
}
