package fluxotel

import (
	"context"
	"encoding/hex"

	"github.com/wotek/flux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func defaultTracer(tracer trace.Tracer) trace.Tracer {
	if tracer != nil {
		return tracer
	}
	return otel.Tracer("github.com/wotek/flux-opentelemetry")
}

func defaultMeter(meter metric.Meter) metric.Meter {
	if meter != nil {
		return meter
	}
	return otel.Meter("github.com/wotek/flux-opentelemetry")
}

// parseSpanContext converts a flux.Instrumentation into an OpenTelemetry remote SpanContext.
func parseSpanContext(inst flux.Instrumentation) (trace.SpanContext, bool) {
	if !inst.IsValid() {
		return trace.SpanContext{}, false
	}

	traceID, err := trace.TraceIDFromHex(inst.TraceID)
	if err != nil {
		return trace.SpanContext{}, false
	}

	spanID, err := trace.SpanIDFromHex(inst.SpanID)
	if err != nil {
		return trace.SpanContext{}, false
	}

	var traceFlags trace.TraceFlags
	if len(inst.TraceFlags) > 0 {
		if b, err := hex.DecodeString(inst.TraceFlags); err == nil && len(b) > 0 {
			traceFlags = trace.TraceFlags(b[0])
		}
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: traceFlags,
		Remote:     true,
	})

	return sc, sc.IsValid()
}

// parentContextWithRemote builds a Go context parented to a remote SpanContext if
// the given flux.Instrumentation is valid and the Go context does not already have an active span.
func parentContextWithRemote(ctx context.Context, inst flux.Instrumentation) context.Context {
	if !inst.IsValid() {
		return ctx
	}

	if trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return ctx
	}

	remoteSC, ok := parseSpanContext(inst)
	if !ok {
		return ctx
	}

	return trace.ContextWithRemoteSpanContext(ctx, remoteSC)
}

// instrumentationFromSpan extracts a vendor-neutral flux.Instrumentation from an active span.
func instrumentationFromSpan(span trace.Span) flux.Instrumentation {
	sc := span.SpanContext()
	if !sc.IsValid() {
		return flux.Instrumentation{}
	}

	return flux.Instrumentation{
		TraceID:    sc.TraceID().String(),
		SpanID:     sc.SpanID().String(),
		TraceFlags: sc.TraceFlags().String(),
	}
}
