package fluxotel

// Status attribute values locked strictly to "ok" or "error".
const (
	// StatusOk indicates a successful operation.
	StatusOk = "ok"

	// StatusError indicates a failed operation.
	StatusError = "error"
)

// OpenTelemetry attribute keys for Flux spans and metrics.
const (
	// AttrCommandType identifies the Go type name of a command.
	AttrCommandType = "flux.command.type"

	// AttrCommandStatus indicates the outcome of command execution ("ok" or "error").
	AttrCommandStatus = "flux.command.status"

	// AttrActor identifies the actor initiating the operation.
	AttrActor = "flux.actor"

	// AttrQueryType identifies the Go type name of a query.
	AttrQueryType = "flux.query.type"

	// AttrQueryStatus indicates the outcome of query execution ("ok" or "error").
	AttrQueryStatus = "flux.query.status"

	// AttrEventStoreStatus indicates the outcome of an event store operation ("ok" or "error").
	AttrEventStoreStatus = "flux.event_store.status"

	// AttrEventStoreOp indicates the specific event store operation ("read" or "stream").
	AttrEventStoreOp = "flux.event_store.operation"

	// AttrEventName identifies the domain event name.
	AttrEventName = "flux.event.name"

	// AttrProjectionStatus indicates the outcome of projection execution ("ok" or "error").
	AttrProjectionStatus = "flux.projection.status"

	// AttrWorkflowStatus indicates the outcome of workflow execution ("ok" or "error").
	AttrWorkflowStatus = "flux.workflow.status"

	// AttrSnapshotStatus indicates the outcome of snapshot store operations ("ok" or "error").
	AttrSnapshotStatus = "flux.snapshot.status"

	// AttrEventID identifies the event unique identifier.
	AttrEventID = "flux.event.id"

	// AttrStreamID identifies the stream unique identifier.
	AttrStreamID = "flux.stream.id"

	// AttrEventRevision identifies the event revision sequence number.
	AttrEventRevision = "flux.event.revision"

	// AttrEventPosition identifies the event global stream position.
	AttrEventPosition = "flux.event.position"

	// AttrWorkflowID identifies the workflow unique identifier.
	AttrWorkflowID = "flux.workflow.id"

	// AttrSnapshotMiss indicates whether a snapshot load resulted in a cache miss.
	AttrSnapshotMiss = "flux.snapshot.miss"

	// AttrProjectionID identifies the projection unique identifier.
	AttrProjectionID = "flux.projection.id"
)

// Metric names following the Flux OpenTelemetry metrics catalog.
const (
	MetricCommandDuration               = "flux.command.duration"
	MetricCommandCount                  = "flux.command.count"
	MetricQueryDuration                 = "flux.query.duration"
	MetricQueryCount                    = "flux.query.count"
	MetricEventStoreAppendDuration      = "flux.event_store.append.duration"
	MetricEventStoreAppendConflicts     = "flux.event_store.append.concurrency_conflicts"
	MetricEventStoreAppendEvents        = "flux.event_store.append.events"
	MetricEventStoreReadDuration        = "flux.event_store.read.duration"
	MetricSnapshotLoadDuration          = "flux.snapshot.load.duration"
	MetricSnapshotSaveDuration          = "flux.snapshot.save.duration"
	MetricSnapshotLoadMisses            = "flux.snapshot.load.misses"
	MetricProjectionStoreUpdateDuration = "flux.projection.store.update.duration"
	MetricProjectionHandlerDuration     = "flux.projection.handler.duration"
	MetricProjectionHandlerCount        = "flux.projection.handler.count"
	MetricWorkflowHandlerDuration       = "flux.workflow.handler.duration"
	MetricWorkflowHandlerCount          = "flux.workflow.handler.count"
)
