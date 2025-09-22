package org.sunbird.learner.actors.coursebatch;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.ContentUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Abstract base class for batch management actors providing common functionality
 * for content validation and collection updates.
 */
public abstract class BaseBatchMgmtActor extends BaseActor {

    private String dateFormat = "yyyy-MM-dd";
    private List<String> validCollectionStatus = Arrays.asList("Live", "Unlisted");
    private String timeZone = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE);

    /**
     * Retrieves collection details for a given activity ID and validates the content.
     *
     * @param requestContext The request context for logging and tracing
     * @param activityId The activity ID to retrieve collection details for
     * @return Map containing collection details
     * @throws ProjectCommonException if the course is invalid or not found
     */
    protected Map<String, Object> getCollectionDetails(RequestContext requestContext, String activityId) {
        Map<String, Object> contentResponse = ContentUtil.getContent(activityId, Arrays.asList("status", "batches", "leafNodesCount"));
        logger.info(requestContext, "BaseBatchMgmtActor:getCollectionDetails: activityId: " + activityId, null,
                contentResponse);
        String status = (String) ((Map<String, Object>)contentResponse.getOrDefault("content", new HashMap<>())).getOrDefault("status", "");
        Integer leafNodesCount = (Integer) ((Map<String, Object>) contentResponse.getOrDefault("content", new HashMap<>())).getOrDefault("leafNodesCount", 0);
        if (null == contentResponse ||
                contentResponse.size() == 0 ||
                !validCollectionStatus.contains(status) || leafNodesCount == 0) {
            logger.info(requestContext, "BaseBatchMgmtActor:getCollectionDetails: Invalid activityId = " + activityId);
            throw new ProjectCommonException(
                    ResponseCode.invalidCourseId.getErrorCode(),
                    ResponseCode.invalidCourseId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        return (Map<String, Object>)contentResponse.getOrDefault("content", new HashMap<>());
    }

    /**
     * Validates activityId and activityType by checking that the object exists and 
     * the primaryCategory matches the activityType.
     *
     * @param requestContext The request context for logging and tracing
     * @param activityId The activity ID (used as courseId/identifier)
     * @param activityType The expected activity type
     * @throws ProjectCommonException if validation fails
     */
    protected void validateActivityIdAndType(RequestContext requestContext, String activityId, String activityType) {
        // Get collection details using activityId as courseId - this will throw exception if object doesn't exist
        Map<String, Object> collectionDetails = getCollectionDetails(requestContext, activityId);
        
        // Extract primaryCategory from content details
        String primaryCategory = (String) collectionDetails.getOrDefault("primaryCategory", "");
        
        logger.info(requestContext, "BaseBatchMgmtActor:validateActivityIdAndType: activityId=" + activityId + 
                ", activityType=" + activityType + ", primaryCategory=" + primaryCategory);

        // Validate primaryCategory matches activityType
        if (!activityType.equalsIgnoreCase(primaryCategory)) {
            logger.info(requestContext, "BaseBatchMgmtActor:validateActivityIdAndType: ActivityType mismatch. Expected: " + 
                    activityType + ", Found: " + primaryCategory + " for activityId: " + activityId);
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestParameter.getErrorCode(),
                    "ActivityType '" + activityType + "' does not match the content's primaryCategory '" + primaryCategory + "'",
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    /**
     * Updates the collection with batch information.
     *
     * @param requestContext The request context for logging and tracing
     * @param courseBatch Map containing course batch data
     * @param contentDetails Map containing content details
     */
    protected void updateCollection(RequestContext requestContext, Map<String, Object> courseBatch, Map<String, Object> contentDetails) {
        List<Map<String, Object>> batches = (List<Map<String, Object>>) contentDetails.getOrDefault("batches", new ArrayList<>());
        Map<String, Object> data =  new HashMap<>();
        data.put("batchId", courseBatch.getOrDefault(JsonKey.BATCH_ID, ""));
        data.put("name", courseBatch.getOrDefault(JsonKey.NAME, ""));
        data.put("createdFor", courseBatch.getOrDefault(JsonKey.COURSE_CREATED_FOR, new ArrayList<>()));
        data.put("startDate", courseBatch.getOrDefault(JsonKey.START_DATE, ""));
        data.put("endDate", courseBatch.getOrDefault(JsonKey.END_DATE, null));
        data.put("enrollmentType", courseBatch.getOrDefault(JsonKey.ENROLLMENT_TYPE, ""));
        data.put("status", courseBatch.getOrDefault(JsonKey.STATUS, ""));
        data.put("enrollmentEndDate", getEnrollmentEndDate((String) courseBatch.getOrDefault(JsonKey.ENROLLMENT_END_DATE, null), (String) courseBatch.getOrDefault(JsonKey.END_DATE, null)));
        batches.removeIf(map -> StringUtils.equalsIgnoreCase((String) courseBatch.getOrDefault(JsonKey.BATCH_ID, ""), (String) map.get("batchId")));
        batches.add(data);
        ContentUtil.updateCollection(requestContext, (String) courseBatch.getOrDefault(JsonKey.COURSE_ID, ""), new HashMap<String, Object>() {{ put("batches", batches);}});
    }

    /**
     * Helper method to calculate enrollment end date based on provided dates.
     *
     * @param enrollmentEndDate The enrollment end date string
     * @param endDate The batch end date string
     * @return The calculated enrollment end date or null if not applicable
     */
    private Object getEnrollmentEndDate(String enrollmentEndDate, String endDate) {
        SimpleDateFormat dateFormatter = ProjectUtil.getDateFormatter(dateFormat);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
        return Optional.ofNullable(enrollmentEndDate).map(x -> x).orElse(Optional.ofNullable(endDate).map(y ->{
            Calendar cal = Calendar.getInstance();
            try {
                cal.setTime(dateFormatter.parse(y));
                cal.add(Calendar.DAY_OF_MONTH, -1);
                return dateFormatter.format(cal.getTime());
            } catch (ParseException e) {
                return null;
            }
        }).orElse(null));
    }
}
