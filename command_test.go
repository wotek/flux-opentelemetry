package fluxotel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wotek/flux"
	fluxotel "github.com/wotek/flux-opentelemetry"
	"github.com/wotek/flux/command"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type sampleCommand struct {
	Value string
}

func setupTelemetry() (*tracetest.InMemoryExporter, trace.Tracer, *sdkmetric.ManualReader, *sdkmetric.MeterProvider) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	tracer := tp.Tracer("test")

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	return exporter, tracer, reader, mp
}

func TestCommandMiddleware(t *testing.T) {
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
			handlerErr:  errors.New("command failed"),
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

			bus := command.NewBus()
			bus.Use(fluxotel.CommandMiddleware(tracer, meter))

			var capturedInst flux.Instrumentation
			command.Register(bus, func(ctx command.Context, cmd sampleCommand) error {
				capturedInst = ctx.Instrumentation()
				return tt.handlerErr
			})

			cmdID := flux.NewIdentifier("test", "", "test", "", "cmd", "1", "")
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

			ctx := command.NewContext(context.Background(), cmdID, actor, corrID, causID, opts...)
			err := command.Execute(ctx, bus, sampleCommand{Value: "hello"})

			if tt.handlerErr != nil && !errors.Is(err, tt.handlerErr) {
				t.Fatalf("expected error %v, got %v", tt.handlerErr, err)
			}
			if tt.handlerErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}

			span := spans[0]
			if span.Name != "flux.command" {
				t.Errorf("span.Name = %q, want %q", span.Name, "flux.command")
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
					case fluxotel.MetricCommandCount:
						foundCount = true
					case fluxotel.MetricCommandDuration:
						foundDuration = true
					}
				}
			}

			if !foundCount {
				t.Errorf("metric %s not found", fluxotel.MetricCommandCount)
			}
			if !foundDuration {
				t.Errorf("metric %s not found", fluxotel.MetricCommandDuration)
			}
		})
	}
}
