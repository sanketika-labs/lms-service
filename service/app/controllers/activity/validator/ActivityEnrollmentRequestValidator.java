package controllers.activity.validator;

import org.apache.commons.collections4.CollectionUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import java.util.List;

public class ActivityEnrollmentRequestValidator extends BaseRequestValidator {

    public ActivityEnrollmentRequestValidator() {}

    public void validateEnrollActivity(Request request) {
        commonValidations(request);
    }

    public void validateUnenrollActivity(Request request) {
        commonValidations(request);
    }

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
        validateActivityType(request);
        validateUserIds(request);
    }

    private void validateActivityType(Request request) {
        String activityType = (String) request.getRequest().get(JsonKey.ACTIVITYTYPE);
        if (!(ProjectUtil.ActivityType.competencyFramework.getVal().equalsIgnoreCase(activityType)
                || ProjectUtil.ActivityType.competencyLevel.getVal().equalsIgnoreCase(activityType))) {
            throw new ProjectCommonException(
                    ResponseCode.invalidParameterValue.getErrorCode(),
                    ResponseCode.invalidParameterValue.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode(),
                    activityType,
                    JsonKey.ACTIVITYTYPE);
        }
    }


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

            @SuppressWarnings("unchecked")
            List<String> userIds = (List<String>) userIdsObj;
            if (CollectionUtils.isEmpty(userIds)) {
                throw new ProjectCommonException(
                        ResponseCode.missingData.getErrorCode(),
                        ResponseCode.missingData.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode(),
                        JsonKey.USER_IDs);
            }

            // Validate each userId is not empty
            for (String userId : userIds) {
                if (userId == null || userId.trim().isEmpty()) {
                    throw new ProjectCommonException(
                            ResponseCode.missingData.getErrorCode(),
                            ResponseCode.missingData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode(),
                            JsonKey.USER_IDs);
                }
            }
        }
    }


}
