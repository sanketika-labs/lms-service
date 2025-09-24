package org.sunbird.learner.actors.activity;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.kafka.client.InstructionEventGenerator;
import org.sunbird.learner.actors.activity.dao.ActivityEnrollmentDao;
import org.sunbird.learner.actors.activity.impl.ActivityBatchDaoImpl;
import org.sunbird.learner.actors.activity.impl.ActivityEnrollmentDaoImpl;
import org.sunbird.learner.actors.coursebatch.BaseBatchMgmtActor;
import org.sunbird.learner.util.Util;
import org.sunbird.models.activity.ActivityUserEnrolment;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ActivityEnrolmentActor extends BaseBatchMgmtActor {

    private ActivityEnrollmentDao activityEnrollmentDao = new ActivityEnrollmentDaoImpl();

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass().getName());

        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "activityEnroll":
                enrollActivity(request);
                break;
            case "activityUnenroll":
                unenrollActivity(request);
                break;
            case "listUserActivityEnrollments":
                listUserActivityEnrollments(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    private void enrollActivity(Request actorMessage) throws Exception {
        Map<String, Object> request = actorMessage.getRequest();
        RequestContext requestContext = actorMessage.getRequestContext();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String activityId = (String) request.get(JsonKey.ACTIVITYID);
        String batchId = (String) request.get(JsonKey.BATCH_ID);
        String activityType = (String) request.get(JsonKey.ACTIVITYTYPE);
        List<String> userIds = (List<String>) request.get("userIds");


        // Validate activity exists and type matches before enrollment
        if (isPrimaryActivityType(activityType)) {
            validateActivityIdAndType(requestContext, activityId, activityType);
        }

        // Activity batch validation removed - allow enrollment without batch validation

        // Process enrollment for all users
        List<Map<String, Object>> enrollmentDataList = new ArrayList<>();
        List<String> alreadyEnrolledUsers = new ArrayList<>();

        for (String userId : userIds) {
            // Check if user is already enrolled
            ActivityUserEnrolment existingEnrollment = activityEnrollmentDao.read(requestContext, userId, activityId, activityType, batchId);

            if (existingEnrollment != null && existingEnrollment.isActive()) {
                alreadyEnrolledUsers.add(userId);
                continue; // Skip already enrolled users
            }

            // Create enrollment data for new enrollment
            Map<String, Object> enrollmentData = createUserActivityEnrollmentMap(userId, activityId, activityType, batchId, existingEnrollment, requestedBy);
            enrollmentDataList.add(enrollmentData);
        }

        // Batch insert all new enrollments
        if (!enrollmentDataList.isEmpty()) {
            activityEnrollmentDao.batchInsert(requestContext, enrollmentDataList);
        }

        // Generate event for CF activities (only for new enrollments)
        if (isPrimaryActivityType(activityType) && !enrollmentDataList.isEmpty()) {
            Map<String, Object> eventData = createBulkEnrollmentEventData(
                    batchId,
                    activityId,
                    activityType,
                    enrollmentDataList,
                    requestedBy,
                    request,
                    "enroll"
            );

            String topic = ProjectUtil.getConfigValue("kafka_activity_batch_topic");
            InstructionEventGenerator.pushInstructionEvent(batchId, topic, eventData);

            logger.info(requestContext,
                    "ActivityEnrolmentActor: enrollActivity - Event published for CF activity. ActivityId: " + activityId +
                            ", batchId: " + batchId + ", enrolled users: " + enrollmentDataList.size());
        } else if (!"CF".equalsIgnoreCase(activityType)) {
            logger.info(requestContext,
                    "ActivityEnrolmentActor: enrollActivity - No event generated for activityType: " + activityType +
                            ". ActivityId: " + activityId + ", batchId: " + batchId + ", enrolled users: " + enrollmentDataList.size());
        }

        // Prepare response
        Response response = new Response();
        response.put(JsonKey.RESPONSE, "SUCCESS");
        response.put(JsonKey.ACTIVITYID, activityId);
        response.put(JsonKey.BATCH_ID, batchId);

        if (!alreadyEnrolledUsers.isEmpty()) {
            response.put("alreadyEnrolledUsers", alreadyEnrolledUsers);
        }

        sender().tell(response, self());

        logger.info(requestContext,
                "ActivityEnrolmentActor: enrollActivity - Bulk enrollment completed. ActivityId: " + activityId +
                        ", batchId: " + batchId + ", activityType: " + activityType +
                        ", new enrollments: " + enrollmentDataList.size() +
                        ", already enrolled: " + alreadyEnrolledUsers.size());
    }

    private void unenrollActivity(Request actorMessage) throws Exception {
        Map<String, Object> request = actorMessage.getRequest();
        RequestContext requestContext = actorMessage.getRequestContext();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String activityId = (String) request.get(JsonKey.ACTIVITYID);
        String batchId = (String) request.get(JsonKey.BATCH_ID);
        String activityType = (String) request.get(JsonKey.ACTIVITYTYPE);
        List<String> userIds = (List<String>) request.get("userIds");


        // Validate activity exists and type matches before unenrollment
        if (isPrimaryActivityType(activityType)) {
            validateActivityIdAndType(requestContext, activityId, activityType);
        }

        // Process unenrollment for all users
        List<String> unenrolledUsers = new ArrayList<>();
        List<String> notEnrolledUsers = new ArrayList<>();
        List<Map<String, Object>> unenrollmentDataList = new ArrayList<>();

        for (String userId : userIds) {
            // Check if user is enrolled
            ActivityUserEnrolment existingEnrollment = activityEnrollmentDao.read(requestContext, userId, activityId, activityType, batchId);

            if (existingEnrollment == null || !existingEnrollment.isActive()) {
                notEnrolledUsers.add(userId);
                continue; // Skip users who are not enrolled
            }

            // Deactivate enrollment
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("active", false);
            updateData.put("datetime", new Timestamp(new Date().getTime()));

            activityEnrollmentDao.update(requestContext, userId, activityId, activityType, batchId, updateData);
            unenrolledUsers.add(userId);

            // Collect data for event generation
            Map<String, Object> userData = new HashMap<>();
            userData.put("userid", userId);
            userData.put("activityid", activityId);
            userData.put("activitytype", activityType);
            userData.put("batchid", batchId);
            unenrollmentDataList.add(userData);
        }

        // Generate event for CF activities (only for unenrolled users)
        if (isPrimaryActivityType(activityType) && !unenrollmentDataList.isEmpty()) {
            Map<String, Object> eventData = createBulkEnrollmentEventData(
                    batchId,
                    activityId,
                    activityType,
                    unenrollmentDataList,
                    requestedBy,
                    request,
                    "unenroll"
            );

            String topic = ProjectUtil.getConfigValue("kafka_activity_batch_topic");
            InstructionEventGenerator.pushInstructionEvent(batchId, topic, eventData);

            logger.info(requestContext,
                    "ActivityEnrolmentActor: unenrollActivity - Event published for CF activity. ActivityId: " + activityId +
                            ", batchId: " + batchId + ", unenrolled users: " + unenrollmentDataList.size());
        } else if (!"CF".equalsIgnoreCase(activityType)) {
            logger.info(requestContext,
                    "ActivityEnrolmentActor: unenrollActivity - No event generated for activityType: " + activityType +
                            ". ActivityId: " + activityId + ", batchId: " + batchId + ", unenrolled users: " + unenrollmentDataList.size());
        }

        // Prepare response
        Response response = new Response();
        response.put(JsonKey.RESPONSE, "SUCCESS");
        response.put(JsonKey.ACTIVITYID, activityId);
        response.put(JsonKey.BATCH_ID, batchId);
        response.put("unenrolledUsers", unenrolledUsers);

        if (!notEnrolledUsers.isEmpty()) {
            response.put("notEnrolledUsers", notEnrolledUsers);
        }

        sender().tell(response, self());

        logger.info(requestContext,
                "ActivityEnrolmentActor: unenrollActivity - Bulk unenrollment completed. ActivityId: " + activityId +
                        ", batchId: " + batchId + ", activityType: " + activityType +
                        ", unenrolled: " + unenrolledUsers.size() +
                        ", not enrolled: " + notEnrolledUsers.size());
    }

    private void listUserActivityEnrollments(Request actorMessage) throws Exception {
        Map<String, Object> request = actorMessage.getRequest();
        RequestContext requestContext = actorMessage.getRequestContext();
        
        // Try to get userId from request body first (for backward compatibility with listEnroll)
        String userId = (String) request.get(JsonKey.USER_ID);
        
        // If not found in request body, get from context (set by BaseController from x-authenticated-user-token header)
        if (userId == null || userId.trim().isEmpty()) {
            userId = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        }

        if (userId == null || userId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.mandatoryParamsMissing.getErrorCode(),
                    "User ID is required. Please provide userId in request body or ensure x-authenticated-user-token header is provided.",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        List<Map<String, Object>> enrollments = activityEnrollmentDao.listUserEnrollments(requestContext, userId, null);

        Response response = new Response();
        Map<String, Object> result = new HashMap<>();
        result.put("enrollments", enrollments != null ? enrollments : new java.util.ArrayList<>());
        result.put(JsonKey.COUNT, enrollments != null ? enrollments.size() : 0);
        result.put(JsonKey.USER_ID, userId);
        response.put(JsonKey.RESPONSE, result);
        sender().tell(response, self());

        logger.info(requestContext,
                "ActivityEnrolmentActor: listUserActivityEnrollments - Listed enrollments for userId: " + userId + 
                ", count: " + (enrollments != null ? enrollments.size() : 0));
    }

    private Map<String, Object> createUserActivityEnrollmentMap(String userId, String activityId, String activityType, String batchId, ActivityUserEnrolment existingEnrollment, String requestedBy) {
        Map<String, Object> enrollmentData = new HashMap<>();
        enrollmentData.put("userid", userId);
        enrollmentData.put("activityid", activityId);
        enrollmentData.put("activitytype", activityType);
        enrollmentData.put("batchid", batchId);
        enrollmentData.put("active", true);

        if (existingEnrollment == null) {
            enrollmentData.put("addedby", requestedBy);
            enrollmentData.put("enrolled_date", new Timestamp(new Date().getTime()));
            enrollmentData.put("status", ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
            enrollmentData.put("datetime", new Timestamp(new Date().getTime()));
            enrollmentData.put("progress", 0);
        }

        return enrollmentData;
    }

    private Map<String, Object> createBulkEnrollmentEventData(
            String batchId,
            String activityId,
            String activityType,
            List<Map<String, Object>> enrollmentDataList,
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
        actor.put("id", "Activity Enrollment Flink Job");
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

        // Object information
        Map<String, Object> object = new HashMap<>();
        object.put("id", activityId);
        String objectType = getObjectTypeFromActivityType(activityType);
        object.put("type", objectType);
        eventData.put("object", object);

        // Event data
        Map<String, Object> edata = new HashMap<>();
        edata.put("action", "activity-" + action.toLowerCase());
        edata.put("batchId", batchId);
        edata.put("activityId", activityId);
        edata.put("activityType", activityType);
        edata.put("requestedBy", requestedBy);
        edata.put("iteration", 1);
        edata.put("userCount", enrollmentDataList.size());

        // Extract user IDs for the event
        List<String> userIds = enrollmentDataList.stream()
                .map(data -> (String) data.get("userid"))
                .collect(Collectors.toList());
        edata.put("userIds", userIds);

        eventData.put("edata", edata);

        return eventData;
    }

    private String getObjectTypeFromActivityType(String activityType) {
        return activityType == null ? "Content" : activityType;
    }

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


}