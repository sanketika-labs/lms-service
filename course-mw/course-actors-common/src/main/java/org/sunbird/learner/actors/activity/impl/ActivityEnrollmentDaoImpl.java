package org.sunbird.learner.actors.activity.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.activity.dao.ActivityEnrollmentDao;
import org.sunbird.learner.util.Util;
import org.sunbird.models.activity.ActivityUserEnrolment;
import org.sunbird.common.models.util.ProjectLogger;

/**
 * Implementation of ActivityEnrollmentDao for Cassandra operations.
 * Handles user enrollment operations for the user_enrolments table.
 */
public class ActivityEnrollmentDaoImpl implements ActivityEnrollmentDao {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo userEnrollmentDb = Util.dbInfoMap.get(JsonKey.ACTIVITY_ENROLMENTS_DB);
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Response create(RequestContext requestContext, Map<String, Object> enrollmentData) {
        // Ensure all required fields are present
        validateEnrollmentData(enrollmentData);

        // Convert field names to match Cassandra column names
        Map<String, Object> cassandraData = CassandraUtil.changeCassandraColumnMapping(enrollmentData);

        return cassandraOperation.insertRecord(
                requestContext,
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                cassandraData);
    }

    @Override
    public Response update(RequestContext requestContext, String userId, String activityId, String activityType, String batchId, Map<String, Object> updateData) {
        // Validate input parameters
        if (userId == null || userId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "userId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (activityId == null || activityId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "activityId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (activityType == null || activityType.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "activityType cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (batchId == null || batchId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "batchId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (updateData == null || updateData.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "updateData cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put("userid", userId);
        primaryKey.put("activityid", activityId);
        primaryKey.put("activitytype", activityType);
        primaryKey.put("batchid", batchId);

        // Remove primary key fields from update data
        Map<String, Object> attributeMap = new HashMap<>();
        attributeMap.putAll(updateData);
        attributeMap.remove("userid");
        attributeMap.remove("activityid");
        attributeMap.remove("activitytype");
        attributeMap.remove("batchid");

        // Validate data types for update fields
        validateOptionalFields(attributeMap);

        // Convert field names to match Cassandra column names
        attributeMap = CassandraUtil.changeCassandraColumnMapping(attributeMap);

        return cassandraOperation.updateRecord(
                requestContext,
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                attributeMap,
                primaryKey);
    }

    @Override
    public ActivityUserEnrolment read(RequestContext requestContext, String userId, String activityId, String activityType, String batchId) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put("userid", userId);
        primaryKey.put("activityid", activityId);
        primaryKey.put("activitytype", activityType);
        primaryKey.put("batchid", batchId);

        Response enrollmentResult = cassandraOperation.getRecordByIdentifier(
                requestContext,
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                primaryKey,
                null);

        List<Map<String, Object>> enrollmentList = (List<Map<String, Object>>) enrollmentResult.get(JsonKey.RESPONSE);

        if (CollectionUtils.isEmpty(enrollmentList)) {
            return null;
        }

        try {
            return mapper.convertValue(enrollmentList.get(0), ActivityUserEnrolment.class);
        } catch (Exception e) {
            ProjectLogger.log("Error converting enrollment data to ActivityUserEnrolment object", e);
            return null;
        }
    }

    @Override
    public Map<String, Object> getEnrollment(RequestContext requestContext, String userId, String activityId, String activityType, String batchId) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put("userid", userId);
        primaryKey.put("activityid", activityId);
        primaryKey.put("activitytype", activityType);
        primaryKey.put("batchid", batchId);

        Response enrollmentResult = cassandraOperation.getRecordByIdentifier(
                requestContext,
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                primaryKey,
                null);

        List<Map<String, Object>> enrollmentList = (List<Map<String, Object>>) enrollmentResult.get(JsonKey.RESPONSE);

        if (CollectionUtils.isEmpty(enrollmentList)) {
            return null;
        }

        return enrollmentList.get(0);
    }

    @Override
    public Response delete(RequestContext requestContext, String userId, String activityId, String activityType, String batchId) {
        // Validate input parameters
        if (userId == null || userId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "userId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (activityId == null || activityId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "activityId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (activityType == null || activityType.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "activityType cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (batchId == null || batchId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "batchId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        Map<String, String> primaryKey = new HashMap<>();
        primaryKey.put("userid", userId);
        primaryKey.put("activityid", activityId);
        primaryKey.put("activitytype", activityType);
        primaryKey.put("batchid", batchId);

        cassandraOperation.deleteRecord(
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                primaryKey,
                requestContext);

        Response response = new Response();
        response.put(JsonKey.RESPONSE, "SUCCESS");
        return response;
    }

    @Override
    public List<String> getActiveUsersInBatch(RequestContext requestContext, String activityId, String activityType, String batchId) {
        return getBatchParticipants(requestContext, activityId, activityType, batchId, true);
    }

    @Override
    public List<String> getBatchParticipants(RequestContext requestContext, String activityId, String activityType, String batchId, boolean activeOnly) {
        // Query by batchid since it's indexed
        Response response = cassandraOperation.getRecordsByIndexedProperty(
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                "batchid",
                batchId,
                requestContext);

        List<Map<String, Object>> enrollmentList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

        if (CollectionUtils.isEmpty(enrollmentList)) {
            return new ArrayList<>();
        }

        return enrollmentList.stream()
                .filter(enrollment -> {
                    // Filter by activityId and activityType
                    String enrollmentActivityId = (String) enrollment.get("activityid");
                    String enrollmentActivityType = (String) enrollment.get("activitytype");

                    if (!activityId.equals(enrollmentActivityId) || !activityType.equals(enrollmentActivityType)) {
                        return false;
                    }

                    // Filter by active status if required
                    if (activeOnly) {
                        Boolean active = (Boolean) enrollment.get("active");
                        return active != null && active;
                    }

                    return true;
                })
                .map(enrollment -> (String) enrollment.get("userid"))
                .collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> listUserEnrollments(RequestContext requestContext, String userId, List<String> activityIdList) {
        // Query by userid since it's the first part of the primary key
        Response response = cassandraOperation.getRecordsByIndexedProperty(
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                "userid",
                userId,
                requestContext);

        List<Map<String, Object>> enrollmentList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

        if (CollectionUtils.isEmpty(enrollmentList)) {
            return new ArrayList<>();
        }

        // Filter by activityIdList if provided
        if (CollectionUtils.isNotEmpty(activityIdList)) {
            return enrollmentList.stream()
                    .filter(enrollment -> {
                        String activityId = (String) enrollment.get("activityid");
                        return activityIdList.contains(activityId);
                    })
                    .collect(Collectors.toList());
        }

        return enrollmentList;
    }

    @Override
    public Response batchInsert(RequestContext requestContext, List<Map<String, Object>> enrollmentList) {
        // Validate all enrollment data
        for (Map<String, Object> enrollmentData : enrollmentList) {
            validateEnrollmentData(enrollmentData);
        }

        // Convert field names to match Cassandra column names
        List<Map<String, Object>> cassandraDataList = enrollmentList.stream()
                .map(CassandraUtil::changeCassandraColumnMapping)
                .collect(Collectors.toList());

        return cassandraOperation.batchInsert(
                requestContext,
                userEnrollmentDb.getKeySpace(),
                userEnrollmentDb.getTableName(),
                cassandraDataList);
    }

    @Override
    public boolean isUserEnrolled(RequestContext requestContext, String userId, String activityId, String activityType, String batchId) {
        ActivityUserEnrolment enrollment = read(requestContext, userId, activityId, activityType, batchId);
        return enrollment != null && enrollment.isActive();
    }

    /**
     * Validates that all required fields are present and have correct data types in enrollment data.
     *
     * @param enrollmentData Enrollment data to validate
     * @throws ProjectCommonException if required fields are missing or have invalid data types
     */
    private void validateEnrollmentData(Map<String, Object> enrollmentData) {
        if (enrollmentData == null) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "Enrollment data cannot be null",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        // Validate required primary key fields
        String[] requiredFields = {"userid", "activityid", "activitytype", "batchid"};

        for (String field : requiredFields) {
            if (!enrollmentData.containsKey(field) || enrollmentData.get(field) == null) {
                throw new ProjectCommonException(
                        ResponseCode.invalidRequestData.getErrorCode(),
                        "Required field '" + field + "' is missing in enrollment data",
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }

            // Validate that required fields are not empty strings
            Object value = enrollmentData.get(field);
            if (value instanceof String && ((String) value).trim().isEmpty()) {
                throw new ProjectCommonException(
                        ResponseCode.invalidRequestData.getErrorCode(),
                        "Required field '" + field + "' cannot be empty",
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }

        // Validate data types for optional fields if present
        validateOptionalFields(enrollmentData);
    }

    /**
     * Validates optional fields for correct data types.
     *
     * @param enrollmentData Enrollment data to validate
     * @throws ProjectCommonException if data types are invalid
     */
    private void validateOptionalFields(Map<String, Object> enrollmentData) {
        // Validate boolean fields
        if (enrollmentData.containsKey("active")) {
            Object activeValue = enrollmentData.get("active");
            if (activeValue != null && !(activeValue instanceof Boolean)) {
                throw new ProjectCommonException(
                        ResponseCode.invalidParameterValue.getErrorCode(),
                        ResponseCode.invalidParameterValue.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        activeValue.toString(),
                        "active");
            }
        }

        // Validate integer fields
        validateIntegerField(enrollmentData, "progress");
        validateIntegerField(enrollmentData, "status");

        // Validate timestamp fields
        validateTimestampField(enrollmentData, "datetime");
        validateTimestampField(enrollmentData, "completedon");
        validateTimestampField(enrollmentData, "enrolled_date");

        // Validate map field
        if (enrollmentData.containsKey("statusmap")) {
            Object statusMapValue = enrollmentData.get("statusmap");
            if (statusMapValue != null && !(statusMapValue instanceof Map)) {
                throw new ProjectCommonException(
                        ResponseCode.invalidParameterValue.getErrorCode(),
                        ResponseCode.invalidParameterValue.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        statusMapValue.toString(),
                        "statusmap");
            }
        }

        // Validate list field
        if (enrollmentData.containsKey("issued_certificates")) {
            Object certificatesValue = enrollmentData.get("issued_certificates");
            if (certificatesValue != null && !(certificatesValue instanceof List)) {
                throw new ProjectCommonException(
                        ResponseCode.invalidParameterValue.getErrorCode(),
                        ResponseCode.invalidParameterValue.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        certificatesValue.toString(),
                        "issued_certificates");
            }
        }
    }

    /**
     * Validates that an integer field has the correct data type.
     */
    private void validateIntegerField(Map<String, Object> enrollmentData, String fieldName) {
        if (enrollmentData.containsKey(fieldName)) {
            Object value = enrollmentData.get(fieldName);
            if (value != null && !(value instanceof Integer) && !(value instanceof Long)) {
                throw new ProjectCommonException(
                        ResponseCode.invalidParameterValue.getErrorCode(),
                        ResponseCode.invalidParameterValue.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        value.toString(),
                        fieldName);
            }
        }
    }

    /**
     * Validates that a timestamp field has the correct data type.
     */
    private void validateTimestampField(Map<String, Object> enrollmentData, String fieldName) {
        if (enrollmentData.containsKey(fieldName)) {
            Object value = enrollmentData.get(fieldName);
            if (value != null && !(value instanceof java.sql.Timestamp) && !(value instanceof java.util.Date) && !(value instanceof Long)) {
                throw new ProjectCommonException(
                        ResponseCode.invalidParameterValue.getErrorCode(),
                        ResponseCode.invalidParameterValue.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        value.toString(),
                        fieldName);
            }
        }
    }

    @Override
    public List<Map<String, Object>> getBatchEnrollments(RequestContext requestContext, String activityId, String activityType, String batchId, boolean activeOnly) {
        // Validate input parameters
        if (activityId == null || activityId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "activityId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (activityType == null || activityType.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "activityType cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (batchId == null || batchId.trim().isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "batchId cannot be null or empty",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        // Query the materialized view using composite primary key
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put("activityid", activityId);
        primaryKey.put("activitytype", activityType);
        primaryKey.put("batchid", batchId);

        Response response = cassandraOperation.getRecordByIdentifier(
                requestContext,
                userEnrollmentDb.getKeySpace(),
                "enrollments_by_batch", // Materialized view table name
                primaryKey,
                null);

        List<Map<String, Object>> enrollmentList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

        if (CollectionUtils.isEmpty(enrollmentList)) {
            return new ArrayList<>();
        }

        // Filter by active status if required
        if (activeOnly) {
            return enrollmentList.stream()
                    .filter(enrollment -> {
                        Boolean active = (Boolean) enrollment.get("active");
                        return active != null && active;
                    })
                    .collect(Collectors.toList());
        }

        return enrollmentList;
    }
}