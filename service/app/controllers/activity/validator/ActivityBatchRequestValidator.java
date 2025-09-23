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
    
    // Singleton instance - thread-safe since the class is stateless
    private static final ActivityBatchRequestValidator INSTANCE = new ActivityBatchRequestValidator();
    
    // Private constructor to prevent direct instantiation
    private ActivityBatchRequestValidator() {
        super();
    }
    
    // Public method to get the singleton instance
    public static ActivityBatchRequestValidator getInstance() {
        return INSTANCE;
    }

    /**
     * Validates the request for creating an activity batch.
     * Checks for mandatory parameters, enrolment type, date validity, and created-for field.
     *
     * @param request the request object containing activity batch details
     * @throws ProjectCommonException if any validation fails
     */
    public void validateCreateActivityBatchRequest(Request request){
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYID),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYID);
        validateParam(
                (String) request.getRequest().get(JsonKey.ACTIVITYTYPE),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYTYPE);
        validateParam(
                (String) request.getRequest().get(JsonKey.NAME),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.NAME);
        validateEnrolmentType(request);
        String startDate = (String) request.getRequest().get(JsonKey.START_DATE);
        validateParam(
                startDate,
                ResponseCode.mandatoryParamsMissing,
                JsonKey.START_DATE
        );
        String endDate = (String) request.getRequest().get(JsonKey.END_DATE);
        String enrollmentEndDate = (String) request.getRequest().get(JsonKey.ENROLLMENT_END_DATE);
        validateDates(startDate, endDate, enrollmentEndDate);
        validateCreatedFor(request);
    }

    /**
     * Validates the request for updating an activity batch.
     * Checks for mandatory parameters, batch status, and date validity.
     *
     * @param request the request object containing activity batch update details
     * @throws ProjectCommonException if any validation fails
     */
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
                (String) request.getRequest().get(JsonKey.ACTIVITYTYPE),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.ACTIVITYTYPE);
        validateParam(
                (String) request.getRequest().get(JsonKey.NAME),
                ResponseCode.mandatoryParamsMissing,
                JsonKey.NAME);
        validateBatchStatus(request);
        String startDate = (String) request.getRequest().get(JsonKey.START_DATE);
        String endDate = (String) request.getRequest().get(JsonKey.END_DATE);
        String enrollmentEndDate = (String) request.getRequest().get(JsonKey.ENROLLMENT_END_DATE);
        validateDates(startDate, endDate, enrollmentEndDate);

    }

    /**
     * Validates the enrolment type in the request. Ensures it is either 'open' or 'invite-only'.
     *
     * @param request the request object containing enrolment type
     * @throws ProjectCommonException if the enrolment type is missing or invalid
     */
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

    /**
     * Validates the provided start, end, and enrollment end dates for an activity batch.
     * Ensures correct format, logical order, and business rules.
     *
     * @param startDate the batch start date (mandatory for create, optional for update)
     * @param endDate the batch end date (optional)
     * @param enrollmentEndDate the enrollment end date (optional)
     * @throws ProjectCommonException if any validation fails
     */
    private void validateDates(String startDate, String endDate, String enrollmentEndDate) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setLenient(false);

        try {
            // Parse startDate
            Date batchStartDate = format.parse(startDate);
            Date todayDate = format.parse(format.format(new Date()));

            if(StringUtils.isNotEmpty(startDate)) {
                // Validate that startDate is today or future
                if (batchStartDate.before(todayDate)) {
                    throw new ProjectCommonException(
                            ResponseCode.courseBatchStartDateError.getErrorCode(),
                            ResponseCode.courseBatchStartDateError.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
            }

            Date batchEndDate = null;
            Date batchEnrollmentEndDate = null;

            // Parse endDate if provided
            if (StringUtils.isNotEmpty(endDate)) {
                batchEndDate = format.parse(endDate);

                // Validate that endDate is after startDate
                if (batchStartDate.getTime() >= batchEndDate.getTime()) {
                    throw new ProjectCommonException(
                            ResponseCode.endDateError.getErrorCode(),
                            ResponseCode.endDateError.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
            }

            // Parse and validate enrollmentEndDate if provided
            if (StringUtils.isNotEmpty(enrollmentEndDate)) {
                batchEnrollmentEndDate = format.parse(enrollmentEndDate);

                // Validate that enrollmentEndDate is after startDate
                if (batchStartDate.getTime() > batchEnrollmentEndDate.getTime()) {
                    throw new ProjectCommonException(
                            ResponseCode.enrollmentEndDateStartError.getErrorCode(),
                            ResponseCode.enrollmentEndDateStartError.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }

                // Validate that enrollmentEndDate is before endDate (if endDate is provided)
                if (StringUtils.isNotEmpty(endDate)
                        && batchEndDate.getTime() < batchEnrollmentEndDate.getTime()) {
                    throw new ProjectCommonException(
                            ResponseCode.enrollmentEndDateEndError.getErrorCode(),
                            ResponseCode.enrollmentEndDateEndError.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
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

    /**
     * Validates that the CREATED_FOR field, if present, is a list.
     *
     * @param request the request object
     * @throws ProjectCommonException if the field is not a list
     */
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

    /**
     * Validates the batch status if provided in the request.
     *
     * @param request the request object
     * @throws ProjectCommonException if the status is invalid
     */
    private void validateBatchStatus(Request request) {
        if (null != request.getRequest().get(JsonKey.STATUS)) {
            boolean status = false;
            status = checkProgressStatus(Integer.parseInt(request.getRequest().get(JsonKey.STATUS).toString()));
            if (!status) {
                throw new ProjectCommonException(
                        ResponseCode.progressStatusError.getErrorCode(),
                        ResponseCode.progressStatusError.getErrorMessage(),
                        ERROR_CODE);
            }
        }
    }

    /**
     * Checks if the provided status value is a valid progress status.
     *
     * @param status the status value to check
     * @return true if valid, false otherwise
     */
    private boolean checkProgressStatus(int status) {
        for (ProjectUtil.ProgressStatus pstatus : ProjectUtil.ProgressStatus.values()) {
            if (pstatus.getValue() == status) {
                return true;
            }
        }
        return false;
    }
}
