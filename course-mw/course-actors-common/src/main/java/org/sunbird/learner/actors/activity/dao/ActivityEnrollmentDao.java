package org.sunbird.learner.actors.activity.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.RequestContext;
import org.sunbird.models.activity.ActivityUserEnrolment;

/**
 * DAO interface for Activity Enrollment operations.
 * Handles CRUD operations for user enrollments in various activity types (CF, CB, CL, etc.)
 * Maps to the user_enrolments table in Cassandra with composite primary key:
 * PRIMARY KEY (userid, activityid, activitytype, batchid)
 */
public interface ActivityEnrollmentDao {

    /**
     * Create user enrollment in activity batch.
     *
     * @param requestContext Request context
     * @param enrollmentData User enrollment information to be created
     * @return Response containing status of enrollment creation
     */
    Response create(RequestContext requestContext, Map<String, Object> enrollmentData);

    /**
     * Update user enrollment information.
     *
     * @param requestContext Request context
     * @param userId User identifier
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @param updateData Enrollment information to be updated
     * @return Response containing status of enrollment update
     */
    Response update(RequestContext requestContext, String userId, String activityId, String activityType, String batchId, Map<String, Object> updateData);

    /**
     * Read user enrollment for given identifiers.
     *
     * @param requestContext Request context
     * @param userId User identifier
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @return User enrollment information
     */
    ActivityUserEnrolment read(RequestContext requestContext, String userId, String activityId, String activityType, String batchId);

    /**
     * Get user enrollment as Map.
     *
     * @param requestContext Request context
     * @param userId User identifier
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @return User enrollment information as Map
     */
    Map<String, Object> getEnrollment(RequestContext requestContext, String userId, String activityId, String activityType, String batchId);

    /**
     * Delete user enrollment.
     *
     * @param requestContext Request context
     * @param userId User identifier
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @return Response containing status of enrollment deletion
     */
    Response delete(RequestContext requestContext, String userId, String activityId, String activityType, String batchId);

    /**
     * Get all active users in a specific activity batch.
     *
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @return List of active user IDs
     */
    List<String> getActiveUsersInBatch(RequestContext requestContext, String activityId, String activityType, String batchId);

    /**
     * Get all users (active and inactive) in a specific activity batch.
     *
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @param activeOnly If true, return only active users; if false, return all users
     * @return List of user IDs
     */
    List<String> getBatchParticipants(RequestContext requestContext, String activityId, String activityType, String batchId, boolean activeOnly);

    /**
     * List all enrollments for a specific user.
     *
     * @param requestContext Request context
     * @param userId User identifier
     * @param activityIdList Optional list of activity IDs to filter by
     * @return List of user enrollments
     */
    List<Map<String, Object>> listUserEnrollments(RequestContext requestContext, String userId, List<String> activityIdList);

    /**
     * Batch insert multiple user enrollments.
     *
     * @param requestContext Request context
     * @param enrollmentList List of enrollment data to insert
     * @return Response containing status of batch insert
     */
    Response batchInsert(RequestContext requestContext, List<Map<String, Object>> enrollmentList);

    /**
     * Check if user is enrolled in the activity batch.
     *
     * @param requestContext Request context
     * @param userId User identifier
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @return true if user is actively enrolled, false otherwise
     */
    boolean isUserEnrolled(RequestContext requestContext, String userId, String activityId, String activityType, String batchId);

    /**
     * Get all users enrolled in a specific activity batch using materialized view.
     * This method queries the enrollments_by_batch materialized view for better performance.
     *
     * @param requestContext Request context
     * @param activityId Activity identifier
     * @param activityType Activity type (CF, CB, CL)
     * @param batchId Batch identifier
     * @param activeOnly If true, return only active users; if false, return all users
     * @return List of user enrollment details
     */
    List<Map<String, Object>> getBatchEnrollments(RequestContext requestContext, String activityId, String activityType, String batchId, boolean activeOnly);
}