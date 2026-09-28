package fluxotel

import (
	"context"
	"fmt"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// WrapProjectionHandler wraps a strongly-typed projection handler function with OpenTelemetry
// tracing and metrics. Callers must wrap their handler functions before registering them with
// projection.RegisterHandler.
//
// Remote Parenting & Ambient Spans:
// The wrapper extracts distributed tracing instrumentation hydrated natively on the event
// context and creates a consumer child span parented to the remote span that produced the event,
// ensuring trace continuity across event log storage. Projection handlers run in asynchronous
// worker contexts; if an ambient span is present (such as an enclosing database transaction from
// a projection store decorator), the event's remote span context takes precedence to maintain
// causal distributed tracing. It reparents the Go context using ctx.WithParent(spanCtx) to
// preserve event coordinates.
func WrapProjectionHandler[E flux.Event](
	tracer trace.Tracer,
	meter metric.Meter,
	handler func(ctx projection.Context, event E) error,
) func(ctx projection.Context, event E) error {
	tr := defaultTracer(tracer)
	m := defaultMeter(meter)

	durationHist, _ := m.Float64Histogram(
		MetricProjectionHandlerDuration,
		metric.WithDescription("Measures the duration of projection handler execution in seconds."),
		metric.WithUnit("s"),
	)

	counter, _ := m.Int64Counter(
		MetricProjectionHandlerCount,
		metric.WithDescription("Counts the total number of projection handler executions."),
		metric.WithUnit("1"),
	)

	return func(ctx projection.Context, event E) error {
		var parentCtx context.Context = ctx
		if remoteSC, ok := parseSpanContext(ctx.Instrumentation()); ok {
			parentCtx = trace.ContextWithRemoteSpanContext(ctx, remoteSC)
		}
		eventName := event.Name()
		if eventName == "" {
			eventName = fmt.Sprintf("%T", event)
		}

		spanOpts := []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String(AttrEventName, eventName),
				attribute.String(AttrEventID, ctx.EventIdentifier().String()),
				attribute.String(AttrStreamID, ctx.Stream().Identifier.String()),
				attribute.Int64(AttrEventRevision, int64(ctx.Revision())),
				attribute.Int64(AttrEventPosition, int64(ctx.Position())),
			),
		}

		spanCtx, span := tr.Start(parentCtx, "flux.projection", spanOpts...)
		defer span.End()

		childCtx := ctx.WithParent(spanCtx)

		start := time.Now()
		err := handler(childCtx, event)
		duration := time.Since(start).Seconds()

		status := StatusOk
		if err != nil {
			status = StatusError
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}

		metricAttrs := metric.WithAttributes(
			attribute.String(AttrEventName, eventName),
			attribute.String(AttrProjectionStatus, status),
		)
		if durationHist != nil {
			durationHist.Record(spanCtx, duration, metricAttrs)
		}
		if counter != nil {
			counter.Add(spanCtx, 1, metricAttrs)
		}

		return err
	}
}
