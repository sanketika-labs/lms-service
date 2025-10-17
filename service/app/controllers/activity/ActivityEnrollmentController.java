package controllers.activity;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.activity.validator.ActivityEnrollmentRequestValidator;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletionStage;

public class ActivityEnrollmentController extends BaseController {

    @Inject
    @Named("activity-enrolment-actor")
    private ActorRef activityEnrolmentActor;

    private static final ActivityEnrollmentRequestValidator validator = ActivityEnrollmentRequestValidator.getInstance();

    /**
     * Handles enrollment requests for an activity batch.
     * Validates the request and delegates to the actor for processing.
     *
     * @param httpRequest the HTTP request containing enrollment details
     * @return a CompletionStage containing the result of the enrollment operation
     */
    public CompletionStage<Result> enrollActivity(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.ENROLL_ACTIVITY.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    validator.validateEnrollActivity(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    /**
     * Handles private enrollment requests for an activity batch.
     * Validates the request and delegates to the actor for processing.
     *
     * @param httpRequest the HTTP request containing private enrollment details
     * @return a CompletionStage containing the result of the private enrollment operation
     */
    public CompletionStage<Result> privateEnrollActivity(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.ENROLL_ACTIVITY.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    validator.validateEnrollActivity(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    /**
     * Handles unenrollment requests for an activity batch.
     * Validates the request and delegates to the actor for processing.
     *
     * @param httpRequest the HTTP request containing unenrollment details
     * @return a CompletionStage containing the result of the unenrollment operation
     */
    public CompletionStage<Result> unenrollActivity(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.UNENROLL_ACTIVITY.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    validator.validateUnenrollActivity(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    /**
     * Handles requests to list user activity enrollments.
     * Extracts userId from x-authenticated-user-token header and delegates to the actor for processing.
     *
     * @param httpRequest the HTTP request
     * @return a CompletionStage containing the list of user activity enrollments
     */
    public CompletionStage<Result> listUserActivityEnrollments(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.LIST_USER_ACTIVITY_ENROLLMENTS.getValue(),
                (request) -> {
                    Request req = (Request) request;
                    validator.validateListUserActivityEnrollments(req);
                    return null;
                },
                httpRequest);
    }

    /**
     * Handles requests to list users enrolled in a specific activity.
     * Validates the request and delegates to the actor for processing.
     *
     * @param httpRequest the HTTP request containing activity details
     * @return a CompletionStage containing the list of enrolled users
     */
    public CompletionStage<Result> listActivityEnrollments(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.LIST_ACTIVITY_ENROLLMENTS.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    validator.validateListActivityEnrollments(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }
}