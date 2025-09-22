package org.sunbird.learner.actors.activity.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.CassandraPropertyReader;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.activity.dao.ActivityBatchDao;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.learner.util.ActivityBatchUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.activity.ActivityBatch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActivityBatchDaoImpl implements ActivityBatchDao {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo activityBatchDb = Util.dbInfoMap.get(JsonKey.ACTIVITY_BATCH_DB);
    private static final CassandraPropertyReader propertiesCache =
            CassandraPropertyReader.getInstance();
    private ObjectMapper mapper = new ObjectMapper();
    private String dateFormat = "yyyy-MM-dd";

    @Override
    public Response create(RequestContext requestContext, ActivityBatch activityBatch) {
        Map<String, Object> map = ActivityBatchUtil.cassandraActivityBatchMapping(activityBatch, dateFormat);
        map = CassandraUtil.changeCassandraColumnMapping(map);
        return cassandraOperation.insertRecord(
                requestContext, activityBatchDb.getKeySpace(), activityBatchDb.getTableName(), map);
    }

    @Override
    public Response update(RequestContext requestContext, String activityId, String batchId, Map<String, Object> map) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.ACTIVITYID, activityId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        Map<String, Object> attributeMap = new HashMap<>();
        attributeMap.putAll(map);
        attributeMap.remove(JsonKey.ACTIVITYID);
        attributeMap.remove(JsonKey.BATCH_ID);
        attributeMap = CassandraUtil.changeCassandraColumnMapping(attributeMap);
        return cassandraOperation.updateRecord(
                requestContext, activityBatchDb.getKeySpace(), activityBatchDb.getTableName(), attributeMap, primaryKey);
    }

    @Override
    public ActivityBatch readById(String activityId, String batchId, RequestContext requestContext) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.ACTIVITYID, activityId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        Response activityBatchResult =
                cassandraOperation.getRecordByIdentifier(
                        requestContext, activityBatchDb.getKeySpace(), activityBatchDb.getTableName(), primaryKey, null);
        List<Map<String, Object>> activityList =
                (List<Map<String, Object>>) activityBatchResult.get(JsonKey.RESPONSE);
        if (activityList.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidCourseBatchId.getErrorCode(),
                    ResponseCode.invalidCourseBatchId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        } else {
            // Remove participant data if present (similar to course batch pattern)
            activityList.get(0).remove(JsonKey.PARTICIPANT);
            return mapper.convertValue(activityList.get(0), ActivityBatch.class);
        }
    }

    @Override
    public Map<String, Object> getActivityBatch(RequestContext requestContext, String activityId, String batchId) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.ACTIVITYID, activityId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        Response activityBatchResult =
                cassandraOperation.getRecordByIdentifier(
                        requestContext, activityBatchDb.getKeySpace(), activityBatchDb.getTableName(), primaryKey, null);
        List<Map<String, Object>> activityList =
                (List<Map<String, Object>>) activityBatchResult.get(JsonKey.RESPONSE);
        return activityList.get(0);
    }

    @Override
    public Response delete(RequestContext requestContext, String batchId) {
        return cassandraOperation.deleteRecord(
                activityBatchDb.getKeySpace(), activityBatchDb.getTableName(), batchId, requestContext);
    }

    @Override
    public void addCertificateTemplateToActivityBatch(
            RequestContext requestContext, String activityId, String batchId, String templateId, Map<String, Object> templateDetails) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.ACTIVITYID, activityId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        cassandraOperation.updateAddMapRecord(
                requestContext, activityBatchDb.getKeySpace(),
                activityBatchDb.getTableName(),
                primaryKey,
                CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN,
                templateId,
                templateDetails);
    }

    @Override
    public void removeCertificateTemplateFromActivityBatch(
            RequestContext requestContext, String activityId, String batchId, String templateId) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.ACTIVITYID, activityId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        cassandraOperation.updateRemoveMapRecord(
                requestContext, activityBatchDb.getKeySpace(),
                activityBatchDb.getTableName(),
                primaryKey,
                CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN,
                templateId);
    }

    @Override
    public java.util.List<java.util.Map<String, Object>> listByActivityId(RequestContext requestContext, String activityId) {
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(JsonKey.ACTIVITYID, activityId);
        Response activityBatchResult = cassandraOperation.getRecordsByCompositeKey(
                activityBatchDb.getKeySpace(), activityBatchDb.getTableName(), compositeKey, requestContext);
        java.util.List<java.util.Map<String, Object>> activityList = (java.util.List<java.util.Map<String, Object>>) activityBatchResult.get(JsonKey.RESPONSE);
        if (activityList == null) {
            activityList = new java.util.ArrayList<>();
        }
        activityList.forEach(m -> m.remove(JsonKey.PARTICIPANT));
        return activityList;
    }

}
