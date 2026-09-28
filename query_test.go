package fluxotel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wotek/flux"
	fluxotel "github.com/wotek/flux-opentelemetry"
	"github.com/wotek/flux/query"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type sampleQuery struct {
	ID string
}

type sampleQueryResult struct {
	Data string
}

func TestQueryMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialInst   flux.Instrumentation
		handlerErr    error
		wantStatus    string
		expectSameID  bool
		wantActorAttr bool
	}{
		{
			name:        "new root trace",
			initialInst: flux.Instrumentation{},
			handlerErr:  nil,
			wantStatus:  fluxotel.StatusOk,
		},
		{
			name: "remote parenting with valid instrumentation",
			initialInst: flux.Instrumentation{
				TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
				SpanID:     "00f067aa0ba902b7",
				TraceFlags: "01",
			},
			handlerErr:   nil,
			wantStatus:   fluxotel.StatusOk,
			expectSameID: true,
		},
		{
			name:        "handler returns error",
			initialInst: flux.Instrumentation{},
			handlerErr:  errors.New("query failed"),
			wantStatus:  fluxotel.StatusError,
		},
		{
			name:          "captures actor attribute",
			initialInst:   flux.Instrumentation{},
			handlerErr:    nil,
			wantStatus:    fluxotel.StatusOk,
			wantActorAttr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exporter, tracer, reader, mp := setupTelemetry()
			meter := mp.Meter("test")

			bus := query.NewBus()
			bus.Use(fluxotel.QueryMiddleware(tracer, meter))

			var capturedInst flux.Instrumentation
			query.Register(bus, func(ctx query.Context, q sampleQuery) (sampleQueryResult, error) {
				capturedInst = ctx.Instrumentation()
				if tt.handlerErr != nil {
					return sampleQueryResult{}, tt.handlerErr
				}
				return sampleQueryResult{Data: "result-" + q.ID}, nil
			})

			qID := flux.NewIdentifier("test", "", "test", "", "query", "1", "")
			actor := flux.Actor{}
			if tt.wantActorAttr {
				actor = flux.Actor{
					Identifier: flux.NewIdentifier("test", "", "test", "", "user", "42", ""),
				}
			}
			corrID := flux.NewIdentifier("test", "", "test", "", "corr", "1", "")
			causID := flux.NewIdentifier("test", "", "test", "", "caus", "1", "")

			var opts []flux.ContextOption
			if tt.initialInst.IsValid() {
				opts = append(opts, flux.WithInstrumentation(tt.initialInst))
			}

			ctx := query.NewContext(context.Background(), qID, actor, corrID, causID, opts...)
			res, err := query.Execute[sampleQuery, sampleQueryResult](ctx, bus, sampleQuery{ID: "100"})

			if tt.handlerErr != nil && !errors.Is(err, tt.handlerErr) {
				t.Fatalf("expected error %v, got %v", tt.handlerErr, err)
			}
			if tt.handlerErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if res.Data != "result-100" {
					t.Errorf("result Data = %q, want %q", res.Data, "result-100")
				}
			}

			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}

			span := spans[0]
			if span.Name != "flux.query" {
				t.Errorf("span.Name = %q, want %q", span.Name, "flux.query")
			}

			if tt.expectSameID {
				if span.SpanContext.TraceID().String() != tt.initialInst.TraceID {
					t.Errorf("TraceID = %s, want %s", span.SpanContext.TraceID().String(), tt.initialInst.TraceID)
				}
				if span.Parent.SpanID().String() != tt.initialInst.SpanID {
					t.Errorf("Parent SpanID = %s, want %s", span.Parent.SpanID().String(), tt.initialInst.SpanID)
				}
			}

			if !capturedInst.IsValid() {
				t.Errorf("expected captured Instrumentation to be valid, got %+v", capturedInst)
			}
			if capturedInst.TraceID != span.SpanContext.TraceID().String() {
				t.Errorf("captured TraceID = %s, want %s", capturedInst.TraceID, span.SpanContext.TraceID().String())
			}
			if capturedInst.SpanID != span.SpanContext.SpanID().String() {
				t.Errorf("captured SpanID = %s, want %s", capturedInst.SpanID, span.SpanContext.SpanID().String())
			}

			// Verify metrics
			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatalf("failed to collect metrics: %v", err)
			}

			foundCount := false
			foundDuration := false
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					switch m.Name {
					case fluxotel.MetricQueryCount:
						foundCount = true
					case fluxotel.MetricQueryDuration:
						foundDuration = true
					}
				}
			}

			if !foundCount {
				t.Errorf("metric %s not found", fluxotel.MetricQueryCount)
			}
			if !foundDuration {
				t.Errorf("metric %s not found", fluxotel.MetricQueryDuration)
			}
		})
	}
}
