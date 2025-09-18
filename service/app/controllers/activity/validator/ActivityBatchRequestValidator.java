package controllers.activity.validator;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ActivityBatchRequestValidator extends BaseRequestValidator {

    private static final int ERROR_CODE = ResponseCode.CLIENT_ERROR.getResponseCode();

    public void validateCreateActivityBatchRequest(Request request){
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYID);
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYTYPE);
        validateParam(
                (String) request.getRequest().get(JsonKey.NAME),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.NAME);
        validateEnrolmentType(request);
        String startDate = (String) request.getRequest().get(JsonKey.START_DATE);
        String endDate = (String) request.getRequest().get(JsonKey.END_DATE);
        String enrollmentEndDate = (String) request.getRequest().get(JsonKey.ENROLLMENT_END_DATE);
        validateStartDate(startDate);
        validateEndDate(startDate, endDate);
        validateEnrollmentEndDate(enrollmentEndDate, startDate, endDate);
        validateCreatedFor(request);
    }

    public void validateUpdateActivityBatchRequest(Request request){
        validateParam(
                (String) request.getRequest().get(JsonKey.BATCH_ID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.BATCH_ID);
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYID);
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYTYPE);
        validateParam(
                (String) request.getRequest().get(JsonKey.NAME),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.NAME);
        validateEnrolmentType(request);
        if (null != request.getRequest().get(JsonKey.STATUS)) {
            boolean status = validateBatchStatus(request);
            if (!status) {
                throw new ProjectCommonException(
                        ResponseCode.progressStatusError.getErrorCode(),
                        ResponseCode.progressStatusError.getErrorMessage(),
                        ERROR_CODE);
            }
        }
        String startDate = (String) request.getRequest().get(JsonKey.START_DATE);
        String endDate = (String) request.getRequest().get(JsonKey.END_DATE);
        String enrollmentEndDate = (String) request.getRequest().get(JsonKey.ENROLLMENT_END_DATE);
        validateStartDate(startDate);
        validateEndDate(startDate, endDate);
        validateEnrollmentEndDate(enrollmentEndDate, startDate, endDate);

    }

    private void validateEnrolmentType(Request request) {
        validateParam(
                (String) request.getRequest().get(JsonKey.ENROLLMENT_TYPE),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ENROLLMENT_TYPE);
        String enrolmentType = (String) request.getRequest().get(JsonKey.ENROLLMENT_TYPE);
        if (!(ProjectUtil.EnrolmentType.open.getVal().equalsIgnoreCase(enrolmentType)
                || ProjectUtil.EnrolmentType.inviteOnly.getVal().equalsIgnoreCase(enrolmentType))) {
            throw new ProjectCommonException(
                    ResponseCode.invalidParameterValue.getErrorCode(),
                    ResponseCode.invalidParameterValue.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode(),
                    enrolmentType,
                    JsonKey.ENROLLMENT_TYPE);
        }
    }

    private void validateStartDate(String startDate) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setLenient(false);
        validateParam(startDate,
                ResponseCode.mandatoryParamsMissing,
                JsonKey.START_DATE);
        try {
            Date batchStartDate = format.parse(startDate);
            Date todayDate = format.parse(format.format(new Date()));
            Calendar cal1 = Calendar.getInstance();
            Calendar cal2 = Calendar.getInstance();
            cal1.setTime(batchStartDate);
            cal2.setTime(todayDate);
            if (batchStartDate.before(todayDate)) {
                throw new ProjectCommonException(
                        ResponseCode.courseBatchStartDateError.getErrorCode(),
                        ResponseCode.courseBatchStartDateError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        } catch (ProjectCommonException e) {
            throw e;
        } catch (Exception e) {
            throw new ProjectCommonException(
                    ResponseCode.dateFormatError.getErrorCode(),
                    ResponseCode.dateFormatError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private static void validateEndDate(String startDate, String endDate) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setLenient(false);
        Date batchEndDate = null;
        Date batchStartDate = null;
        try {
            if (StringUtils.isNotEmpty(endDate)) {
                batchEndDate = format.parse(endDate);
                batchStartDate = format.parse(startDate);
            }
        } catch (Exception e) {
            throw new ProjectCommonException(
                    ResponseCode.dateFormatError.getErrorCode(),
                    ResponseCode.dateFormatError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (StringUtils.isNotEmpty(endDate) && batchStartDate.getTime() >= batchEndDate.getTime()) {
            throw new ProjectCommonException(
                    ResponseCode.endDateError.getErrorCode(),
                    ResponseCode.endDateError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private static void validateEnrollmentEndDate(String enrollmentEndDate, String startDate, String endDate) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setLenient(false);
        Date batchEndDate = null;
        Date batchStartDate = null;
        Date batchenrollmentEndDate = null;
        try {
            if (StringUtils.isNotEmpty(enrollmentEndDate)) {
                batchenrollmentEndDate = format.parse(enrollmentEndDate);
                batchStartDate = format.parse(startDate);
            }
            if (StringUtils.isNotEmpty(endDate)) {
                batchEndDate = format.parse(endDate);
            }

        } catch (Exception e) {
            throw new ProjectCommonException(
                    ResponseCode.dateFormatError.getErrorCode(),
                    ResponseCode.dateFormatError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (StringUtils.isNotEmpty(enrollmentEndDate)
                && batchStartDate.getTime() > batchenrollmentEndDate.getTime()) {
            throw new ProjectCommonException(
                    ResponseCode.enrollmentEndDateStartError.getErrorCode(),
                    ResponseCode.enrollmentEndDateStartError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (StringUtils.isNotEmpty(enrollmentEndDate)
                && StringUtils.isNotEmpty(endDate)
                && batchEndDate.getTime() < batchenrollmentEndDate.getTime()) {
            throw new ProjectCommonException(
                    ResponseCode.enrollmentEndDateEndError.getErrorCode(),
                    ResponseCode.enrollmentEndDateEndError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private void validateCreatedFor(Request request) {
        if (request.getRequest().containsKey(JsonKey.COURSE_CREATED_FOR)
                && !(request.getRequest().get(JsonKey.COURSE_CREATED_FOR) instanceof List)) {
            throw new ProjectCommonException(
                    ResponseCode.dataTypeError.getErrorCode(),
                    ResponseCode.dataTypeError.getErrorMessage(),
                    ERROR_CODE,
                    JsonKey.COURSE_CREATED_FOR,
                    "Arrays");
        }
    }

    private boolean validateBatchStatus(Request request) {
        boolean status = false;
        try {
            status = checkProgressStatus(Integer.parseInt(request.getRequest().get(JsonKey.STATUS).toString()));
        } catch (Exception e) {
            logger.error(null, e.getMessage(), e);
        }
        return status;
    }

    private boolean checkProgressStatus(int status) {
        for (ProjectUtil.ProgressStatus pstatus : ProjectUtil.ProgressStatus.values()) {
            if (pstatus.getValue() == status) {
                return true;
            }
        }
        return false;
    }
}
