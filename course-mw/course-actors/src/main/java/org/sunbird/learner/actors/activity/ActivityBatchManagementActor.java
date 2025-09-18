package org.sunbird.learner.actors.activity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.base.BaseActor;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ActivityBatchManagementActor extends BaseActor {

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
                createActivityBatch(request);
                break;
            case "updateBatch":
                updateActivityBatch(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    private void createActivityBatch(Request actorMessage) throws Exception {

        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String batchId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        String activityId = (String) request.get(JsonKey.ACTIVITYID);
        String activityType = (String) request.get(JsonKey.ACTIVITYTYPE);
        ActivityBatch activityBatch = JsonUtil.convert(request, ActivityBatch.class);
        activityBatch.setStatus(setActivityBatchStatus((String) request.get(JsonKey.START_DATE)));
        activityBatch.setCreatedDate(ProjectUtil.getTimeStamp());
        if (StringUtils.isBlank(activityBatch.getCreatedBy())) {
            activityBatch.setCreatedBy(requestedBy);
        }
        activityBatch.setBatchId(batchId);
        
        // Validate createdFor organizations
        validateContentOrg(actorMessage.getRequestContext(), activityBatch.getCreatedFor());

        Response result = activityBatchDao.create(actorMessage.getRequestContext(), activityBatch);

        //Generating an event into Kafka
        if ("CF".equalsIgnoreCase(activityType)) {
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
        
        // Read existing batch to validate permissions
        ActivityBatch existingBatch = activityBatchDao.readById(activityId, batchId, actorMessage.getRequestContext());
        validateUserPermission(existingBatch, requestedBy);
        
        ActivityBatch activityBatch = JsonUtil.convert(request, ActivityBatch.class);
        
        // Validate createdFor organizations if being updated
        validateContentOrg(actorMessage.getRequestContext(), activityBatch.getCreatedFor());
        activityBatch.setUpdatedDate(ProjectUtil.getTimeStamp());
        Map<String, Object> updateData = new HashMap<>();
        updateData.putAll(request);
        updateData.remove(JsonKey.BATCH_ID);
        updateData.remove(JsonKey.ACTIVITYID);
        updateData.put("updatedDate", ProjectUtil.getTimeStamp());

        Response result = activityBatchDao.update(actorMessage.getRequestContext(), activityId, batchId, updateData);

        if ("CF".equalsIgnoreCase(activityType)) {
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
            String topic = ProjectUtil.getConfigValue("kafka_topics_batch_instruction");
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

    private String getObjectTypeFromActivityType(String activityType) {
        if (activityType == null) {
            return "Content";
        }

        switch (activityType.toUpperCase()) {
            case "CF":
                return "Competency Framework";
            case "CL":
                return "Competency Level";
            case "CB":
                return "Course";
            default:
                return "Content";
        }
    }

    private void validateUserPermission(ActivityBatch activityBatch, String requestedBy) {
        // List<String> canUpdateList = new ArrayList<>();
        // if (StringUtils.isNotBlank(activityBatch.getCreatedBy())) {
        //     canUpdateList.add(activityBatch.getCreatedBy());
        // }

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
}
