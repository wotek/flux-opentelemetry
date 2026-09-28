package fluxotel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wotek/flux"
	fluxotel "github.com/wotek/flux-opentelemetry"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/workflow"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

type testWorkflow struct {
	id flux.Identifier
}

func (w *testWorkflow) Identifier() flux.Identifier {
	return w.id
}

func (w *testWorkflow) New() *testWorkflow {
	return &testWorkflow{}
}

func (w *testWorkflow) Clone() *testWorkflow {
	if w == nil {
		return nil
	}
	cp := *w
	return &cp
}

type orderPlacedEvent struct {
	OrderID string
}

func (o orderPlacedEvent) Name() string { return "order.placed" }

type shipOrderCommand struct {
	OrderID string
	Carrier string
}

func TestWrapWorkflowHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		inst         flux.Instrumentation
		handlerErr   error
		wantStatus   string
		expectParent bool
	}{
		{
			name: "remote parented from event instrumentation",
			inst: flux.Instrumentation{
				TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
				SpanID:     "00f067aa0ba902b7",
				TraceFlags: "01",
			},
			handlerErr:   nil,
			wantStatus:   fluxotel.StatusOk,
			expectParent: true,
		},
		{
			name:         "handler returns error",
			inst:         flux.Instrumentation{},
			handlerErr:   errors.New("workflow step failed"),
			wantStatus:   fluxotel.StatusError,
			expectParent: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exporter, tracer, reader, mp := setupTelemetry()
			meter := mp.Meter("test")

			var capturedTraceID string
			var capturedSpanValid bool
			wrapped := fluxotel.WrapWorkflowHandler(tracer, meter, func(ctx workflow.Context, wf *testWorkflow, e orderPlacedEvent) error {
				span := trace.SpanFromContext(ctx)
				capturedSpanValid = span.SpanContext().IsValid()
				capturedTraceID = span.SpanContext().TraceID().String()

				if e.OrderID != "order-999" {
					t.Errorf("OrderID = %q, want %q", e.OrderID, "order-999")
				}
				if wf.Identifier().String() != "urn:test::::order:wf:1" {
					t.Errorf("Workflow ID = %q, want %q", wf.Identifier().String(), "urn:test::::order:wf:1")
				}

				// Enqueue command on reparented context
				workflow.EnqueueCommand(ctx, shipOrderCommand{OrderID: e.OrderID, Carrier: "fedex"})

				return tt.handlerErr
			})

			meta := make(map[string]string)
			if tt.inst.IsValid() {
				meta[flux.MetadataTraceID] = tt.inst.TraceID
				meta[flux.MetadataSpanID] = tt.inst.SpanID
				meta[flux.MetadataTraceFlags] = tt.inst.TraceFlags
			}

			env := flux.Envelope{
				Identifier: flux.NewIdentifier("test", "", "test", "", "event", "1", ""),
				Stream:     flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")},
				Revision:   1,
				Event:      orderPlacedEvent{OrderID: "order-999"},
				Metadata:   meta,
			}

			eventCtx := event.NewContext(context.Background(), env)
			wfCtx := workflow.NewContext(eventCtx)

			wfInstance := &testWorkflow{
				id: flux.NewIdentifier("test", "", "", "", "order:wf", "1", ""),
			}

			err := wrapped(wfCtx, wfInstance, orderPlacedEvent{OrderID: "order-999"})
			if tt.handlerErr != nil && !errors.Is(err, tt.handlerErr) {
				t.Fatalf("expected error %v, got %v", tt.handlerErr, err)
			}
			if tt.handlerErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !capturedSpanValid {
				t.Errorf("expected active span in handler context")
			}

			// Verify command was enqueued on wfCtx despite child context reparenting
			queued := wfCtx.QueuedCommands()
			if len(queued) != 1 {
				t.Fatalf("expected 1 queued command, got %d", len(queued))
			}
			cmd, ok := queued[0].(shipOrderCommand)
			if !ok || cmd.OrderID != "order-999" {
				t.Errorf("expected shipOrderCommand with OrderID order-999, got %+v", queued[0])
			}

			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}

			span := spans[0]
			if span.Name != "flux.workflow" {
				t.Errorf("span.Name = %q, want %q", span.Name, "flux.workflow")
			}
			if span.SpanKind != trace.SpanKindConsumer {
				t.Errorf("span.SpanKind = %v, want %v", span.SpanKind, trace.SpanKindConsumer)
			}

			if tt.expectParent {
				if span.SpanContext.TraceID().String() != tt.inst.TraceID {
					t.Errorf("TraceID = %s, want %s", span.SpanContext.TraceID().String(), tt.inst.TraceID)
				}
				if span.Parent.SpanID().String() != tt.inst.SpanID {
					t.Errorf("Parent SpanID = %s, want %s", span.Parent.SpanID().String(), tt.inst.SpanID)
				}
				if capturedTraceID != tt.inst.TraceID {
					t.Errorf("captured TraceID = %s, want %s", capturedTraceID, tt.inst.TraceID)
				}
			}

			if tt.handlerErr != nil {
				if span.Status.Code != codes.Error {
					t.Errorf("span status code = %v, want %v", span.Status.Code, codes.Error)
				}
			} else {
				if span.Status.Code != codes.Ok {
					t.Errorf("span status code = %v, want %v", span.Status.Code, codes.Ok)
				}
			}

			// Verify metrics
			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatalf("failed to collect metrics: %v", err)
			}

			foundDuration := false
			foundCount := false
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					switch m.Name {
					case fluxotel.MetricWorkflowHandlerDuration:
						foundDuration = true
					case fluxotel.MetricWorkflowHandlerCount:
						foundCount = true
					}
				}
			}

			if !foundDuration {
				t.Errorf("metric %s not found", fluxotel.MetricWorkflowHandlerDuration)
			}
			if !foundCount {
				t.Errorf("metric %s not found", fluxotel.MetricWorkflowHandlerCount)
			}
		})
	}
}
