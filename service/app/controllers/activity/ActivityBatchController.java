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

    // Static final field for cleaner and more efficient access
    private static final ActivityBatchRequestValidator validator = ActivityBatchRequestValidator.getInstance();

    /**
     * Handles the creation of a new activity batch.
     * Delegates to createBatchWrapper with idFromRequest set to false.
     *
     * @param httpRequest the HTTP request containing batch creation details
     * @return a CompletionStage containing the result of the batch creation
     */
    public CompletionStage<Result> createBatch(Http.Request httpRequest) {
        return createBatchWrapper(httpRequest, false);
    }

    /**
     * Handles the creation of a new private activity batch.
     * Delegates to createBatchWrapper with idFromRequest set to true.
     *
     * @param httpRequest the HTTP request containing batch creation details
     * @return a CompletionStage containing the result of the private batch creation
     */
    public CompletionStage<Result> privateCreateBatch(Http.Request httpRequest) {
        return createBatchWrapper(httpRequest, true);
    }

    /**
     * Wrapper method for creating an activity batch (public or private).
     * Validates the request and delegates to the appropriate actor operation.
     *
     * @param httpRequest the HTTP request containing batch creation details
     * @param idFromRequest flag indicating if the batch is private
     * @return a CompletionStage containing the result of the batch creation
     */
    private CompletionStage<Result> createBatchWrapper(Http.Request httpRequest, boolean idFromRequest) {
        String operation = idFromRequest ? ActorOperations.PRIVATE_CREATE_BATCH.getValue() : ActorOperations.CREATE_BATCH.getValue();
        return handleRequest(
                activityBatchActorRef,
                operation,
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    validator.validateCreateActivityBatchRequest(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest
        );
    }

    /**
     * Handles the update of an existing activity batch.
     * Validates the request and delegates to the update actor operation.
     *
     * @param httpRequest the HTTP request containing batch update details
     * @return a CompletionStage containing the result of the batch update
     */
    public CompletionStage<Result> updateBatch(Http.Request httpRequest) {
        return handleRequest(
                activityBatchActorRef,
                ActorOperations.UPDATE_BATCH.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    validator.validateUpdateActivityBatchRequest(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest
        );
    }

    /**
     * Retrieves the list of batches for a given activity ID.
     *
     * @param activityId the ID of the activity
     * @param httpRequest the HTTP request
     * @return a CompletionStage containing the result with the list of batches
     */
    public CompletionStage<Result> listBatches(String activityId, Http.Request httpRequest) {
        return handleRequest(
                activityBatchActorRef,
                ActorOperations.GET_ACTIVITY_BATCHES.getValue(),
                activityId,
                JsonKey.ACTIVITYID,
                false,
                httpRequest
        );
    }

}
