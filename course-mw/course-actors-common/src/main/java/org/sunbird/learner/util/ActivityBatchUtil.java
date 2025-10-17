package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.models.activity.ActivityBatch;
import scala.concurrent.Future;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.TimeZone;


/**
 * Utility class for ActivityBatch operations including Elasticsearch synchronization.
 * Similar to CourseBatchUtil but for ActivityBatch entities.
 */
public class ActivityBatchUtil {
  private static ElasticSearchService esUtil = EsClientFactory.getInstance(JsonKey.REST);
  private static ObjectMapper mapper = new ObjectMapper();
  private static LoggerUtil logger = new LoggerUtil(ActivityBatchUtil.class);
  private static final List<String> changeInDateFormat = JsonKey.CHANGE_IN_DATE_FORMAT;
  private static final List<String> changeInSimpleDateFormat = JsonKey.CHANGE_IN_SIMPLE_DATE_FORMAT;
  private static final List<String> changeInDateFormatAll = JsonKey.CHANGE_IN_DATE_FORMAT_ALL;
  private static final List<String> setEndOfDay = JsonKey.SET_END_OF_DAY;

  private ActivityBatchUtil() {}

  /**
   * Synchronizes activity batch data to Elasticsearch in foreground.
   * 
   * @param requestContext the request context
   * @param uniqueId the unique identifier for the activity batch
   * @param req the activity batch data to sync
   */
  public static void syncActivityBatchForeground(RequestContext requestContext, String uniqueId, Map<String, Object> req) {
    logger.info(requestContext, "ActivityBatchManagementActor: syncActivityBatchForeground called for activity batch ID = "
            + uniqueId);
    req.put(JsonKey.ID, uniqueId);
    req.put(JsonKey.IDENTIFIER, uniqueId);
    Future<String> esResponseF =
        esUtil.save(requestContext, ProjectUtil.EsType.activityBatch.getTypeName(), uniqueId, req);
    String esResponse = (String) ElasticSearchHelper.getResponseFromFuture(esResponseF);
    logger.info(requestContext, "ActivityBatchManagementActor::syncActivityBatchForeground: Sync response for activity batch ID = "
            + uniqueId
            + " received response = "
            + esResponse);
  }

  /**
   * Validates if an activity batch exists in Elasticsearch.
   * 
   * @param requestContext the request context
   * @param activityId the activity ID
   * @param batchId the batch ID
   * @return the activity batch data if found
   * @throws ProjectCommonException if batch doesn't exist or is not linked to activity
   */
  public static Map<String, Object> validateActivityBatch(RequestContext requestContext, String activityId, String batchId) {
    Future<Map<String, Object>> resultF =
        esUtil.getDataByIdentifier(requestContext, EsType.activityBatch.getTypeName(), batchId);
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(resultF);
    if (MapUtils.isEmpty(result)) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.CLIENT_ERROR, "No such batchId exists");
    }
    if (StringUtils.isNotBlank(activityId)
        && !StringUtils.equals(activityId, (String) result.get(JsonKey.ACTIVITYID))) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.CLIENT_ERROR, "batchId is not linked with activityId");
    }
    return result;
  }

  /**
   * Creates Elasticsearch mapping for ActivityBatch similar to CourseBatchUtil.esCourseMapping.
   * 
   * @param activityBatch the ActivityBatch object to map
   * @param pattern the date format pattern
   * @return Map containing the mapped activity batch data for ES
   * @throws Exception if date formatting fails
   */
  public static Map<String, Object> esActivityBatchMapping(ActivityBatch activityBatch, String pattern) throws Exception {
    SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
    SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
    dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    @SuppressWarnings("unchecked")
    Map<String, Object> esActivityBatchMap = mapper.convertValue(activityBatch, Map.class);
    
    // Format date fields for ES
    changeInDateFormat.forEach(key -> {
      if (null != esActivityBatchMap.get(key))
        esActivityBatchMap.put(key, dateTimeFormat.format(esActivityBatchMap.get(key)));
      else 
        esActivityBatchMap.put(key, null);
    });
    
    changeInSimpleDateFormat.forEach(key -> {
      if (null != esActivityBatchMap.get(key))
        esActivityBatchMap.put(key, dateFormat.format(esActivityBatchMap.get(key)));
      else 
        esActivityBatchMap.put(key, null);
    });
    
    // Add certificate templates if present
    esActivityBatchMap.put("certTemplates", activityBatch.getCertTemplates());
    
    return esActivityBatchMap;
  }

  /**
   * Creates Cassandra mapping for ActivityBatch with proper date formatting.
   * 
   * @param activityBatch the ActivityBatch object to map
   * @param pattern the date format pattern
   * @return Map containing the mapped activity batch data for Cassandra
   */
  public static Map<String, Object> cassandraActivityBatchMapping(ActivityBatch activityBatch, String pattern) {
    SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
    SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
    dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    @SuppressWarnings("unchecked")
    Map<String, Object> activityBatchMap = mapper.convertValue(activityBatch, Map.class);
    
    changeInDateFormatAll.forEach(key -> {
      try {
        if (activityBatchMap.containsKey(key))
          activityBatchMap.put(key, setEndOfDay(key, dateTimeFormat.parse(dateTimeFormat.format(activityBatchMap.get(key))), dateFormat));
      } catch (ParseException e) {
        logger.error(null, "ActivityBatchUtil:cassandraActivityBatchMapping: Exception occurred with message = " + e.getMessage(), e);
      }
    });
    return activityBatchMap;
  }

  /**
   * Sets end of day (23:59:59:999) for specific date fields like endDate and enrollmentEndDate.
   * 
   * @param key the field name
   * @param value the date value
   * @param dateFormat the date formatter
   * @return the date with end of day time set
   */
  private static Date setEndOfDay(String key, Date value, SimpleDateFormat dateFormat) {
    try {
      if (setEndOfDay.contains(key)) {
        Calendar cal =
                Calendar.getInstance(
                        TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
        cal.setTime(dateFormat.parse(dateFormat.format(value)));
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        return cal.getTime();
      }
    } catch (ParseException e) {
      logger.error(null, "ActivityBatchUtil:setEndOfDay: Exception occurred with message = " + e.getMessage(), e);
    }
    return value;
  }
}