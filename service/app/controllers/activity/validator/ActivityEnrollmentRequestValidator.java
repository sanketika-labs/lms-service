package controllers.activity.validator;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import java.util.List;

public class ActivityEnrollmentRequestValidator extends BaseRequestValidator {

    private static final ActivityEnrollmentRequestValidator INSTANCE = new ActivityEnrollmentRequestValidator();

    private ActivityEnrollmentRequestValidator() {
        super();
    }

    /**
     * Returns the singleton instance of ActivityEnrollmentRequestValidator.
     *
     * @return the singleton instance
     */
    public static ActivityEnrollmentRequestValidator getInstance() {
        return INSTANCE;
    }

    /**
     * Validates the request for enrolling in an activity.
     * Checks for mandatory parameters and user IDs.
     *
     * @param request the request object containing enrollment details
     * @throws ProjectCommonException if any validation fails
     */
    public void validateEnrollActivity(Request request) {
        commonValidations(request);
    }

    /**
     * Validates the request for unenrolling from an activity.
     * Checks for mandatory parameters and user IDs.
     *
     * @param request the request object containing unenrollment details
     * @throws ProjectCommonException if any validation fails
     */
    public void validateUnenrollActivity(Request request) {
        commonValidations(request);
    }

    /**
     * Performs common validations for enroll and unenroll requests.
     * Validates activity ID, activity type, batch ID, and user IDs.
     *
     * @param request the request object
     * @throws ProjectCommonException if any validation fails
     */
    private void commonValidations(Request request) {
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYID);
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYTYPE),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYTYPE);
        validateParam(
                (String) request.getRequest().get(JsonKey.BATCH_ID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.BATCH_ID);
        validateUserIds(request);
    }

    /**
     * Validates the user IDs in the request. Ensures the field is a non-empty list of non-empty strings.
     *
     * @param request the request object
     * @throws ProjectCommonException if the user IDs are missing, not a list, or contain invalid entries
     */
    private void validateUserIds(Request request) {
        Object userIdsObj = request.getRequest().get(JsonKey.USER_IDs);
        if (userIdsObj != null) {
            if (!(userIdsObj instanceof List)) {
                throw new ProjectCommonException(
                        ResponseCode.dataTypeError.getErrorCode(),
                        ResponseCode.dataTypeError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        JsonKey.USER_IDs
                );
            }

            List<String> userIds = (List<String>) userIdsObj;
            if (CollectionUtils.isEmpty(userIds)) {
                throw new ProjectCommonException(
                        ResponseCode.missingData.getErrorCode(),
                        ResponseCode.missingData.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        JsonKey.USER_IDs);
            }

            for (String userId : userIds) {
                if (StringUtils.isEmpty(userId) || userId.trim().isEmpty()) {
                    throw new ProjectCommonException(
                            ResponseCode.missingData.getErrorCode(),
                            ResponseCode.missingData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode(),
                            JsonKey.USER_IDs);
                }
            }
        }
    }

    /**
     * Validates the request for listing user activity enrollments.
     * Validates that userId is present in the context (extracted from x-authenticated-user-token header).
     *
     * @param request the request object containing enrollment list details
     * @throws ProjectCommonException if any validation fails
     */
    public void validateListUserActivityEnrollments(Request request) {
        // Validate that userId is present in context (extracted from x-authenticated-user-token header)
        String userId = (String) request.getContext().get(JsonKey.REQUESTED_BY);
        if (StringUtils.isEmpty(userId)) {
            throw new ProjectCommonException(
                    ResponseCode.mandatoryParamsMissing.getErrorCode(),
                    ResponseCode.mandatoryParamsMissing.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode(),
                    JsonKey.REQUESTED_BY);
        }
    }

    /**
     * Validates the request for listing users enrolled in an activity.
     * Validates activity ID, activity type, and batch ID are present.
     *
     * @param request the request object containing activity details
     * @throws ProjectCommonException if any validation fails
     */
    public void validateListActivityEnrollments(Request request) {
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYID);
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYTYPE),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYTYPE);
        validateParam(
                (String) request.getRequest().get(JsonKey.BATCH_ID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.BATCH_ID);
    }

}