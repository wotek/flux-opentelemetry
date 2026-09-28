package fluxotel

import (
	"context"
	"fmt"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/workflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// WrapWorkflowHandler wraps a strongly-typed workflow handler function with OpenTelemetry
// tracing and metrics. Callers must wrap their handler functions before registering them with
// workflow.RegisterHandler.
//
// Remote Parenting & Ambient Spans:
// The wrapper extracts distributed tracing instrumentation hydrated natively on the event
// context and creates a consumer child span parented to the remote span that produced the event,
// ensuring trace continuity across event log storage. Workflow handlers run in asynchronous
// worker contexts; if an ambient span is present, the event's remote span context takes
// precedence to maintain causal distributed tracing. It reparents the Go context using
// ctx.WithParent(spanCtx), which preserves event metadata and shares the mutable command
// queue pointer so enqueued outbox commands continue the distributed trace.
func WrapWorkflowHandler[W workflow.Workflow[W], E flux.Event](
	tracer trace.Tracer,
	meter metric.Meter,
	handler func(ctx workflow.Context, wf W, event E) error,
) func(ctx workflow.Context, wf W, event E) error {
	tr := defaultTracer(tracer)
	m := defaultMeter(meter)

	durationHist, _ := m.Float64Histogram(
		MetricWorkflowHandlerDuration,
		metric.WithDescription("Measures the duration of workflow handler execution in seconds."),
		metric.WithUnit("s"),
	)

	counter, _ := m.Int64Counter(
		MetricWorkflowHandlerCount,
		metric.WithDescription("Counts the total number of workflow handler executions."),
		metric.WithUnit("1"),
	)

	return func(ctx workflow.Context, wf W, event E) error {
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
			),
		}
		var zero W
		if any(wf) != any(zero) {
			if wfID := wf.Identifier().String(); wfID != "" {
				spanOpts = append(spanOpts, trace.WithAttributes(attribute.String(AttrWorkflowID, wfID)))
			}
		}

		spanCtx, span := tr.Start(parentCtx, "flux.workflow", spanOpts...)
		defer span.End()

		childCtx := ctx.WithParent(spanCtx)

		start := time.Now()
		err := handler(childCtx, wf, event)
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
			attribute.String(AttrWorkflowStatus, status),
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
