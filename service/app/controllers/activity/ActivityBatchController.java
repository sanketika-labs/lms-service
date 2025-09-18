package controllers.activity;

import controllers.BaseController;
import controllers.activity.validator.ActivityBatchRequestValidator;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletionStage;

public class ActivityBatchController  extends BaseController {
    
    @Inject
    @Named("activity-batch-management-actor")
    private akka.actor.ActorRef activityBatchActorRef;

    public CompletionStage<Result> createBatch(Http.Request httpRequest) {
        return createBatchWrapper(httpRequest, false);
    }

    public CompletionStage<Result> privateCreateBatch(Http.Request httpRequest) {
        return createBatchWrapper(httpRequest, true);
    }

    private CompletionStage<Result> createBatchWrapper(Http.Request httpRequest, boolean idFromRequest) {
        String operation = idFromRequest ? ActorOperations.PRIVATE_CREATE_BATCH.getValue() : ActorOperations.CREATE_BATCH.getValue();
        return handleRequest(
                activityBatchActorRef,
                operation,
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    new ActivityBatchRequestValidator().validateCreateActivityBatchRequest(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest
        );
    }

    public CompletionStage<Result> updateBatch(Http.Request httpRequest) {
        return handleRequest(
                activityBatchActorRef,
                ActorOperations.UPDATE_BATCH.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    new ActivityBatchRequestValidator().validateUpdateActivityBatchRequest(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest
        );
    }

}
