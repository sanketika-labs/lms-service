package controllers.courseenrollment.validator;

import org.sunbird.common.models.util.*;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.exception.ProjectCommonException;

public class CourseEnrollmentRequestValidator extends BaseRequestValidator {

  public CourseEnrollmentRequestValidator() {}

  public void validateEnrollCourse(Request courseRequestDto) {
    commonValidations(courseRequestDto);
  }

  public void validateUnenrollCourse(Request courseRequestDto) {
    commonValidations(courseRequestDto);
  }

  private void commonValidations(Request courseRequestDto) {
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.COURSE_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.COURSE_ID+"/"+JsonKey.COLLECTION_ID);
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.BATCH_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.BATCH_ID);
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.USER_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.USER_ID);
  }

  public void validateEnrolledCourse(Request courseRequestDto) {
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.BATCH_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.BATCH_ID);
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.USER_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.USER_ID);
  }

  public void validateUserEnrolledCourse(Request courseRequestDto) {
    validateParam(
            (String) courseRequestDto.get(JsonKey.USER_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.USER_ID);
  }

  public void validateBulkEnrollCourse(Request courseRequestDto) {
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.COURSE_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.COURSE_ID+"/"+JsonKey.COLLECTION_ID);
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.BATCH_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.BATCH_ID);
    
    // Validate userIds list instead of single userId
    Object userIdsObj = courseRequestDto.getRequest().get("userIds");
    if (userIdsObj == null) {
      throw new ProjectCommonException(
          ResponseCode.mandatoryParamsMissing.getErrorCode(),
          "userIds parameter is missing",
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    
    if (!(userIdsObj instanceof java.util.List)) {
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          "userIds must be a list",
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    
    java.util.List<?> userIds = (java.util.List<?>) userIdsObj;
    if (userIds.isEmpty()) {
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          "userIds list cannot be empty",
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    
    // Validate that all elements in the list are strings
    for (Object userId : userIds) {
      if (!(userId instanceof String) || ((String) userId).trim().isEmpty()) {
        throw new ProjectCommonException(
            ResponseCode.invalidRequestData.getErrorCode(),
            "All userIds must be non-empty strings",
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }
}
