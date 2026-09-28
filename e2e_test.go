package fluxotel_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	fluxotel "github.com/wotek/flux-opentelemetry"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/projection"
	projstore "github.com/wotek/flux/projection/store"
	"github.com/wotek/flux/workflow"
	workflowstore "github.com/wotek/flux/workflow/store"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Domain types for E2E acceptance test.

type OrderEvent interface {
	flux.Event
	isOrderEvent()
}

type OrderPlaced struct {
	OrderID string
}

func (e OrderPlaced) Name() string  { return "order.placed" }
func (e OrderPlaced) isOrderEvent() {}

type OrderAggregate struct {
	flux.AggregateRoot[OrderEvent]
	OrderID string
}

func (a *OrderAggregate) New(stream flux.Stream) *OrderAggregate {
	return NewOrderAggregate(stream)
}

func NewOrderAggregate(stream flux.Stream) *OrderAggregate {
	a := &OrderAggregate{}
	a.AggregateRoot = flux.NewAggregateRoot[OrderEvent](stream, flux.NewChangeset[OrderEvent](), a.apply)
	return a
}

func (a *OrderAggregate) apply(e OrderEvent) {
	switch evt := e.(type) {
	case OrderPlaced:
		a.OrderID = evt.OrderID
	}
}

func (a *OrderAggregate) PlaceOrder(orderID string) {
	evt := OrderPlaced{OrderID: orderID}
	a.apply(evt)
	a.Changeset().Record(evt)
}

type OrderWorkflow struct {
	id flux.Identifier
}

func (w *OrderWorkflow) Identifier() flux.Identifier {
	return w.id
}

func (w *OrderWorkflow) New() *OrderWorkflow {
	return &OrderWorkflow{}
}

func (w *OrderWorkflow) Clone() *OrderWorkflow {
	if w == nil {
		return nil
	}
	cp := *w
	return &cp
}

type CreateOrderCommand struct {
	OrderID string
}

type SendConfirmationEmailCommand struct {
	OrderID   string
	Recipient string
}

func TestE2E_Acceptance_TraceContinuity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// 1. Setup OpenTelemetry Tracer & Meter
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	tracer := tp.Tracer("flux-e2e")

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("flux-e2e")

	// 2. Storage & Bus Wiring with OTel wrappers
	rawES := eventstore.New()
	es := fluxotel.WrapEventStore(rawES, tracer, meter)

	cmdBus := command.NewBus()
	cmdBus.Use(fluxotel.CommandMiddleware(tracer, meter))

	rawProjStore := projstore.New()
	pStore := fluxotel.WrapProjectionStore(rawProjStore, tracer, meter)

	wfStore := workflowstore.New[*OrderWorkflow](cmdBus)

	// 3. Register Command Handlers
	repo := flux.NewAggregateRepository[*OrderAggregate, OrderEvent](es)

	command.Register(cmdBus, func(cmdCtx command.Context, cmd CreateOrderCommand) error {
		streamID := flux.NewIdentifier("flux", "", "order", "", "stream", cmd.OrderID, "")
		stream := flux.Stream{Identifier: streamID}
		agg := NewOrderAggregate(stream)
		agg.PlaceOrder(cmd.OrderID)
		return repo.Save(cmdCtx, agg)
	})

	outboxHandled := make(chan string, 1)
	command.Register(cmdBus, func(cmdCtx command.Context, cmd SendConfirmationEmailCommand) error {
		outboxHandled <- cmdCtx.Instrumentation().TraceID
		return nil
	})

	// 4. Setup Projector with WrapProjectionHandler
	projID := flux.NewIdentifier("flux", "", "order", "", "projection", "orders", "")
	proj := projection.New(projID, es, pStore)

	projectionHandled := make(chan struct{}, 1)
	projection.RegisterHandler(proj, fluxotel.WrapProjectionHandler(tracer, meter, func(pCtx projection.Context, e OrderPlaced) error {
		select {
		case projectionHandled <- struct{}{}:
		default:
		}
		return nil
	}))

	// 5. Setup Workflow Orchestrator with WrapWorkflowHandler
	orchID := flux.NewIdentifier("flux", "", "order", "", "orchestrator", "fulfillment", "")
	orch := workflow.New(orchID, es, nil)

	workflow.RegisterHandler(orch, wfStore, fluxotel.WrapWorkflowHandler(tracer, meter, func(wfCtx workflow.Context, wf *OrderWorkflow, e OrderPlaced) error {
		workflow.EnqueueCommand(wfCtx, SendConfirmationEmailCommand{OrderID: e.OrderID, Recipient: "customer@example.com"})
		return nil
	}))

	// 6. Start Background Workers (Projector, Orchestrator, Outbox Relay)
	go func() { _ = proj.Start(ctx) }()
	go func() { _ = orch.Start(ctx) }()
	wfStore.StartRelay(ctx)

	// 7. Dispatch the Initial Command
	initialCmdID := flux.NewIdentifier("flux", "", "order", "", "cmd", "create-1", "")
	initialActor := flux.Actor{
		Identifier: flux.NewIdentifier("flux", "", "auth", "", "user", "customer-42", ""),
	}
	initialCorrID := flux.NewIdentifier("flux", "", "order", "", "corr", "flow-101", "")
	initialCausID := flux.NewIdentifier("flux", "", "order", "", "caus", "entry-1", "")

	rootCmdCtx := command.NewContext(
		ctx,
		initialCmdID,
		initialActor,
		initialCorrID,
		initialCausID,
	)

	err := command.Execute(rootCmdCtx, cmdBus, CreateOrderCommand{OrderID: "order-xyz"})
	if err != nil {
		t.Fatalf("failed to execute initial command: %v", err)
	}

	// 8. Wait for Asynchronous Workers to Process
	select {
	case <-projectionHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for projection handler to run")
	}

	var outboxTraceID string
	select {
	case outboxTraceID = <-outboxHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for outbox command to be dispatched and handled")
	}

	// Stop background workers
	cancel()

	// 9. Inspect Collected Spans
	var spans []tracetest.SpanStub
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		spans = exporter.GetSpans()
		var hasInitial, hasOutbox, hasProj, hasWf bool
		for _, s := range spans {
			if s.Name == "flux.command" {
				for _, attr := range s.Attributes {
					if attr.Key == fluxotel.AttrCommandType {
						if attr.Value.AsString() == "fluxotel_test.CreateOrderCommand" {
							hasInitial = true
						}
						if attr.Value.AsString() == "fluxotel_test.SendConfirmationEmailCommand" {
							hasOutbox = true
						}
					}
				}
			}
			if s.Name == "flux.projection" {
				hasProj = true
			}
			if s.Name == "flux.workflow" {
				hasWf = true
			}
		}
		if hasInitial && hasOutbox && hasProj && hasWf {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(spans) == 0 {
		t.Fatal("expected spans to be captured, got none")
	}

	var (
		initialCommandSpan *tracetest.SpanStub
		outboxCommandSpan  *tracetest.SpanStub
		projectionSpan     *tracetest.SpanStub
		workflowSpan       *tracetest.SpanStub
		eventStoreSpans    []*tracetest.SpanStub
	)

	for i := range spans {
		s := &spans[i]
		switch s.Name {
		case "flux.command":
			for _, attr := range s.Attributes {
				if attr.Key == fluxotel.AttrCommandType {
					if attr.Value.AsString() == "fluxotel_test.CreateOrderCommand" {
						initialCommandSpan = s
					} else if attr.Value.AsString() == "fluxotel_test.SendConfirmationEmailCommand" {
						outboxCommandSpan = s
					}
				}
			}
		case "flux.projection":
			projectionSpan = s
		case "flux.workflow":
			workflowSpan = s
		case "flux.event_store.append":
			eventStoreSpans = append(eventStoreSpans, s)
		}
	}

	if initialCommandSpan == nil {
		t.Fatal("initial command span ('CreateOrderCommand') not found")
	}
	if outboxCommandSpan == nil {
		t.Fatal("outbox-dispatched command span ('SendConfirmationEmailCommand') not found")
	}
	if projectionSpan == nil {
		for _, s := range spans {
			t.Logf("Captured span: %s (attrs: %v)", s.Name, s.Attributes)
		}
		t.Fatal("projection span not found")
	}
	if workflowSpan == nil {
		t.Fatal("workflow span not found")
	}
	if len(eventStoreSpans) == 0 {
		t.Fatal("event store append span not found")
	}

	expectedTraceID := initialCommandSpan.SpanContext.TraceID().String()

	// CRITICAL ASSERTION:
	// The outbox-dispatched command span TraceID MUST exactly match the original command's TraceID!
	outboxSpanTraceID := outboxCommandSpan.SpanContext.TraceID().String()
	if outboxSpanTraceID != expectedTraceID {
		t.Errorf("CRITICAL TRACE CONTINUITY FAILURE: outbox command TraceID = %s, want %s (original command TraceID)",
			outboxSpanTraceID, expectedTraceID)
	}

	if outboxTraceID != expectedTraceID {
		t.Errorf("dispatched command context TraceID = %s, want %s", outboxTraceID, expectedTraceID)
	}

	// CRITICAL ASSERTION:
	// The outbox command span must be parented by the initial command span!
	initialSpanID := initialCommandSpan.SpanContext.SpanID()
	outboxParent := outboxCommandSpan.Parent
	if !outboxParent.IsValid() || outboxParent.SpanID() != initialSpanID {
		t.Errorf("outbox command parent SpanID = %v, want %v (initial command span)",
			outboxParent.SpanID(), initialSpanID)
	}

	// Verify all asynchronous spans share the original TraceID and are parented by initial command span
	if projectionSpan.SpanContext.TraceID().String() != expectedTraceID {
		t.Errorf("projection span TraceID = %s, want %s",
			projectionSpan.SpanContext.TraceID().String(), expectedTraceID)
	}
	if !projectionSpan.Parent.IsValid() || projectionSpan.Parent.SpanID() != initialSpanID {
		t.Errorf("projection parent SpanID = %v, want %v", projectionSpan.Parent.SpanID(), initialSpanID)
	}

	if workflowSpan.SpanContext.TraceID().String() != expectedTraceID {
		t.Errorf("workflow span TraceID = %s, want %s",
			workflowSpan.SpanContext.TraceID().String(), expectedTraceID)
	}
	if !workflowSpan.Parent.IsValid() || workflowSpan.Parent.SpanID() != initialSpanID {
		t.Errorf("workflow parent SpanID = %v, want %v", workflowSpan.Parent.SpanID(), initialSpanID)
	}

	for _, esSpan := range eventStoreSpans {
		if esSpan.SpanContext.TraceID().String() != expectedTraceID {
			t.Errorf("event store span TraceID = %s, want %s",
				esSpan.SpanContext.TraceID().String(), expectedTraceID)
		}
	}

	t.Logf("End-to-End Acceptance Test Passed! All spans verified under unified TraceID: %s", expectedTraceID)
}
