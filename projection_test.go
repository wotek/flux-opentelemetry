package fluxotel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wotek/flux"
	fluxotel "github.com/wotek/flux-opentelemetry"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/projection"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

type userCreatedEvent struct {
	UserID string
}

func (u userCreatedEvent) Name() string { return "user.created" }

func TestWrapProjectionHandler(t *testing.T) {
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
			handlerErr:   errors.New("projection failed"),
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
			wrapped := fluxotel.WrapProjectionHandler(tracer, meter, func(ctx projection.Context, e userCreatedEvent) error {
				span := trace.SpanFromContext(ctx)
				capturedSpanValid = span.SpanContext().IsValid()
				capturedTraceID = span.SpanContext().TraceID().String()

				if e.UserID != "user-123" {
					t.Errorf("UserID = %q, want %q", e.UserID, "user-123")
				}
				if ctx.Revision() != 1 {
					t.Errorf("Revision = %d, want 1", ctx.Revision())
				}
				if ctx.Position() != 5 {
					t.Errorf("Position = %d, want 5", ctx.Position())
				}

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
				Position:   5,
				Event:      userCreatedEvent{UserID: "user-123"},
				Metadata:   meta,
			}

			eventCtx := event.NewContext(context.Background(), env)
			projCtx := projection.NewContext(eventCtx)

			err := wrapped(projCtx, userCreatedEvent{UserID: "user-123"})
			if tt.handlerErr != nil && !errors.Is(err, tt.handlerErr) {
				t.Fatalf("expected error %v, got %v", tt.handlerErr, err)
			}
			if tt.handlerErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !capturedSpanValid {
				t.Errorf("expected active span in handler context")
			}

			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}

			span := spans[0]
			if span.Name != "flux.projection" {
				t.Errorf("span.Name = %q, want %q", span.Name, "flux.projection")
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
					case fluxotel.MetricProjectionHandlerDuration:
						foundDuration = true
					case fluxotel.MetricProjectionHandlerCount:
						foundCount = true
					}
				}
			}

			if !foundDuration {
				t.Errorf("metric %s not found", fluxotel.MetricProjectionHandlerDuration)
			}
			if !foundCount {
				t.Errorf("metric %s not found", fluxotel.MetricProjectionHandlerCount)
			}
		})
	}
}
