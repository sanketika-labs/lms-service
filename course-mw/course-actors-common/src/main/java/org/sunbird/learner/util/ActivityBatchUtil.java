package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.models.activity.ActivityBatch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ActivityBatchUtil {

    private static ObjectMapper mapper = new ObjectMapper();
    private static LoggerUtil logger = new LoggerUtil(ActivityBatchUtil.class);
    private static final List<String> changeInDateFormat = JsonKey.CHANGE_IN_DATE_FORMAT;
    private static final List<String> changeInSimpleDateFormat = JsonKey.CHANGE_IN_SIMPLE_DATE_FORMAT;
    private static final List<String> changeInDateFormatAll = JsonKey.CHANGE_IN_DATE_FORMAT_ALL;
    private static final List<String> setEndOfDay = JsonKey.SET_END_OF_DAY;

    /**
     * Method to change the timestamp (Long) into date with valid format for Cassandra
     */
    public static Map<String, Object> cassandraActivityBatchMapping(ActivityBatch activityBatch, String pattern) {
        SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
        SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
        dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
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
     * Method to add endOfDay (23:59:59:999) in endDate and enrollmentEndDate
     */
    private static Date setEndOfDay(String key, Date value, SimpleDateFormat dateFormat) {
        try {
            if (setEndOfDay.contains(key)) {
                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
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
