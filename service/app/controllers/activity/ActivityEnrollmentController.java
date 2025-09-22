package controllers.activity;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.activity.validator.ActivityEnrollmentRequestValidator;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
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

    public CompletionStage<Result> enrollActivity(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.ENROLL_ACTIVITY.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    new ActivityEnrollmentRequestValidator().validateEnrollActivity(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> privateEnrollActivity(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.ENROLL_ACTIVITY.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    new ActivityEnrollmentRequestValidator().validateEnrollActivity(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> unenrollActivity(Http.Request httpRequest) {
        return handleRequest(
                activityEnrolmentActor,
                ActorOperations.UNENROLL_ACTIVITY.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    new ActivityEnrollmentRequestValidator().validateUnenrollActivity(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }
}
