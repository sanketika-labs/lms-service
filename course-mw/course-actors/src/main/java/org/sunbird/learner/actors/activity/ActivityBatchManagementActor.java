package org.sunbird.learner.actors.activity;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.sunbird.learner.util.Util;
import org.sunbird.models.activity.ActivityBatch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ActivityBatchManagementActor extends BaseBatchMgmtActor {

    private ActivityBatchDao activityBatchDao = new ActivityBatchDaoImpl();
    private UserOrgService userOrgService = UserOrgServiceImpl.getInstance();
    private ObjectMapper mapper = new ObjectMapper();
    private String dateFormat = "yyyy-MM-dd";
    private String timeZone = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE);

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

    private String composeBatchId(Request request, boolean fromReq) {
        String provided = fromReq ? (String) request.get(JsonKey.BATCH_ID) : null;
        if (StringUtils.isBlank(provided)) {
            return ProjectUtil.getUniqueIdFromTimestamp(request.getEnv());
        }
        return provided;
    }

    private void listActivityBatches(Request actorMessage) {
        String activityId = (String) actorMessage.getContext().get(JsonKey.ACTIVITYID);
        // Validate activityId by ensuring collection exists and is valid
        getCollectionDetails(actorMessage.getRequestContext(), activityId);
        java.util.List<java.util.Map<String, Object>> result = activityBatchDao.listByActivityId(actorMessage.getRequestContext(), activityId);
        Response response = new Response();
        response.put(JsonKey.RESPONSE, result);
        sender().tell(response, self());
    }

    private void createActivityBatch(Request actorMessage, boolean idFromRequest) throws Exception {
        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String batchId = composeBatchId(actorMessage, idFromRequest);
        String activityId = (String) request.get(JsonKey.ACTIVITYID);
        String activityType = (String) request.get(JsonKey.ACTIVITYTYPE);
        
        // Validate activity exists and type matches before creating batch
        validateActivityExistsAndType(actorMessage.getRequestContext(), activityId, activityType);

        Map<String, String> headers = (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);
        
        // Validate activityId and activityType
        Map<String, Object> contentDetails = validateActivityIdAndType(actorMessage.getRequestContext(), activityId, activityType);
        
        // Get collection details for updateCollection call

        
        ActivityBatch activityBatch = JsonUtil.convert(request, ActivityBatch.class);
        activityBatch.setStatus(setActivityBatchStatus((String) request.get(JsonKey.START_DATE)));
        activityBatch.setCreatedDate(ProjectUtil.getTimeStamp());
        
        // Handle createdBy field with priority: request body > auth token > system
        String createdBy = determineCreatedBy(request, requestedBy);
        request.put(JsonKey.CREATED_BY, createdBy);
        activityBatch.setCreatedBy(createdBy);
        activityBatch.setBatchId(batchId);
        
        // Validate createdFor organizations
        validateContentOrg(actorMessage.getRequestContext(), activityBatch.getCreatedFor());

        Response result = activityBatchDao.create(actorMessage.getRequestContext(), activityBatch);

        // Create activity batch mapping for updateCollection
        Map<String, Object> esActivityBatchMap = createActivityBatchMapping(activityBatch, dateFormat);
    
        // Update collection metadata with batch information
        updateCollection(actorMessage.getRequestContext(), esActivityBatchMap, contentDetails);

        //Generating an event into Kafka for configured primary activity types
        if (shouldTriggerBatchEvent(activityType)) {
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

            logger.info(actorMessage.getRequestContext(),
                    "ActivityBatchManagementActor: createBatch - Event published for CF activity. ActivityId: " + activityId + ", batchId: " + batchId);
        } else {
            logger.info(actorMessage.getRequestContext(),
                    "ActivityBatchManagementActor: createBatch - No event generated for activityType: " + activityType + ". ActivityId: " + activityId + ", batchId: " + batchId);
        }

        result.put(JsonKey.BATCH_ID, batchId);
        sender().tell(result, self());

        logger.info(actorMessage.getRequestContext(),
                "ActivityBatchManagementActor: createBatch - ActivityBatch created for activityId: " + activityId + ", batchId: " + batchId + ", activityType: " + activityType);

    }

    private void updateActivityBatch(Request actorMessage) throws Exception {

        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String batchId = (String) request.get(JsonKey.BATCH_ID);
        String activityId = (String) request.get(JsonKey.ACTIVITYID);
        String activityType = (String) request.get("activityType");
        Map<String, String> headers = (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);
        
        // Validate activityId and activityType
        Map<String, Object> contentDetails = validateActivityIdAndType(actorMessage.getRequestContext(), activityId, activityType);
        
        
        // Validate activity exists and type matches before updating batch
        validateActivityExistsAndType(actorMessage.getRequestContext(), activityId, activityType);
        
        // Read existing batch to validate permissions
        ActivityBatch existingBatch = activityBatchDao.readById(activityId, batchId, actorMessage.getRequestContext());
        validateUserPermission(existingBatch, requestedBy);
        
        ActivityBatch activityBatch = JsonUtil.convert(request, ActivityBatch.class);
        
        // Validate createdFor organizations if being updated
        validateContentOrg(actorMessage.getRequestContext(), activityBatch.getCreatedFor());
        activityBatch.setUpdatedDate(ProjectUtil.getTimeStamp());
        
        // Create update data with only valid table columns
        Map<String, Object> updateData = new HashMap<>();
        
        // Only include fields that exist in the batches table
        if (request.containsKey(JsonKey.NAME)) {
            updateData.put(JsonKey.NAME, request.get(JsonKey.NAME));
        }
        if (request.containsKey(JsonKey.DESCRIPTION)) {
            updateData.put(JsonKey.DESCRIPTION, request.get(JsonKey.DESCRIPTION));
        }
        if (request.containsKey(JsonKey.START_DATE)) {
            updateData.put(JsonKey.START_DATE, convertDateStringToTimestamp((String) request.get(JsonKey.START_DATE)));
        }
        if (request.containsKey(JsonKey.END_DATE)) {
            updateData.put(JsonKey.END_DATE, convertDateStringToTimestamp((String) request.get(JsonKey.END_DATE)));
        }
        if (request.containsKey(JsonKey.ENROLLMENT_END_DATE)) {
            updateData.put(JsonKey.ENROLLMENT_END_DATE, convertDateStringToTimestamp((String) request.get(JsonKey.ENROLLMENT_END_DATE)));
        }
        if (request.containsKey(JsonKey.ENROLLMENT_TYPE)) {
            updateData.put(JsonKey.ENROLLMENT_TYPE, request.get(JsonKey.ENROLLMENT_TYPE));
        }
        if (request.containsKey(JsonKey.STATUS)) {
            updateData.put(JsonKey.STATUS, request.get(JsonKey.STATUS));
        }
        if (request.containsKey(JsonKey.COURSE_CREATED_FOR)) {
            updateData.put(JsonKey.COURSE_CREATED_FOR, request.get(JsonKey.COURSE_CREATED_FOR));
        }
        if (request.containsKey(JsonKey.CERT_TEMPLATES)) {
            updateData.put(JsonKey.CERT_TEMPLATES, request.get(JsonKey.CERT_TEMPLATES));
        }
        
        updateData.put("updatedDate", ProjectUtil.getTimeStamp());

        Response result = activityBatchDao.update(actorMessage.getRequestContext(), activityId, batchId, updateData);

        // Create updated activity batch mapping and update collection metadata
        ActivityBatch updatedBatch = activityBatchDao.readById(activityId, batchId, actorMessage.getRequestContext());
        Map<String, Object> esActivityBatchMap = createActivityBatchMapping(updatedBatch, dateFormat);
        updateCollection(actorMessage.getRequestContext(), esActivityBatchMap, contentDetails);

        //Generating an event into Kafka for configured primary activity types
        if (shouldTriggerBatchEvent(activityType)) {
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

            logger.info(actorMessage.getRequestContext(),
                    "ActivityBatchManagementActor: updateBatch - Event published for CF activity. BatchId: " + batchId + ", activityId: " + activityId);
        } else {
            logger.info(actorMessage.getRequestContext(),
                    "ActivityBatchManagementActor: updateBatch - No event generated for activityType: " + activityType + ". BatchId: " + batchId + ", activityId: " + activityId);
        }

        result.put(JsonKey.BATCH_ID, batchId);
        sender().tell(result, self());

        logger.info(actorMessage.getRequestContext(),
                "ActivityBatchManagementActor: updateBatch - ActivityBatch updated for batchId: " + batchId + ", activityId: " + activityId + ", activityType: " + activityType);
    }

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
        actor.put("id", "Batch and Enrollment Flink Job");
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
                "enrollmentEndDate", "status", "createdFor", "createdBy"};

        for (String field : supportedFields) {
            if (requestData.containsKey(field)) {
                edata.put(field, requestData.get(field));
            }
        }
        
        // Add requestedBy (authenticated user) to event data
        edata.put("requestedBy", requestedBy);

        eventData.put("edata", edata);

        return eventData;
    }

    private String getObjectTypeFromActivityType(String activityType) {
        return (activityType == null) ? "Content" : activityType;        
    }

    private boolean shouldTriggerBatchEvent(String activityType) {
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
     * Determines the createdBy value with priority:
     * 1. Request body (if provided)
     * 2. Authenticated user from token
     * 3. Default system value
     *
     * @param request The request object containing potential createdBy field
     * @param requestedBy The authenticated user ID from token
     * @return The determined createdBy value
     */
    private String determineCreatedBy(Map<String, Object> request, String requestedBy) {
        // Priority 1: Check if createdBy is provided in request body
        if (request.containsKey(JsonKey.CREATED_BY)) {
            String requestCreatedBy = (String) request.get(JsonKey.CREATED_BY);
            if (requestCreatedBy != null && !requestCreatedBy.trim().isEmpty()) {
                return requestCreatedBy.trim();
            }
        }
        
        // Priority 2: Use authenticated user from token
        if (requestedBy != null && !requestedBy.trim().isEmpty()) {
            return requestedBy.trim();
        }
        
        // Priority 3: Default to system
        return "system";
    }
    
    /**
     * Convert date string (yyyy-MM-dd) to timestamp for Cassandra
     */
    private Long convertDateStringToTimestamp(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return null;
        }
        
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
            Date date = dateFormat.parse(dateString.trim());
            return date.getTime();
        } catch (Exception e) {
            logger.error(null, "ActivityBatchManagementActor:convertDateStringToTimestamp: Exception occurred while parsing date: " + dateString + ", error: " + e.getMessage(), e);
            throw new ProjectCommonException(
                    ResponseCode.invalidParameterValue.getErrorCode(),
                    ResponseCode.invalidParameterValue.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode(),
                    dateString,
                    "date");
        }
    }
    
    /**
     * Validates that the activity exists in the database and its type matches the expected type.
     * TODO: Implement proper activity validation logic
     * 
     * @param requestContext Request context
     * @param activityId Activity identifier to validate
     * @param activityType Expected activity type
     */
    private void validateActivityExistsAndType(RequestContext requestContext, String activityId, String activityType) {
        // TODO: Implement activity validation logic
        // 1. Check if activity exists in database
        // 2. Verify activity type matches expected type
        // 3. Ensure activity is in valid state
        /**
         * Creates a mapping of ActivityBatch for elasticsearch/collection updates.
         * Similar to CourseBatchUtil.esCourseMapping but for ActivityBatch.
         *
         * @param activityBatch The ActivityBatch object to map
         * @param dateFormat The date format to use for date fields
         * @return Map containing the mapped activity batch data
         */
    }
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
