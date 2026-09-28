package fluxotel

import (
	"fmt"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// CommandMiddleware returns a command bus middleware that traces command executions
// and records duration and throughput metrics.
//
// Remote Parenting:
// If the command context carries valid Instrumentation and the Go context does not
// already have an active span (e.g., when dispatched via an asynchronous Outbox relay),
// the middleware reconstructs a remote SpanContext to ensure the command span seamlessly
// continues the originating distributed trace.
func CommandMiddleware(tracer trace.Tracer, meter metric.Meter, opts ...Option) command.Middleware {
	tr := defaultTracer(tracer)
	m := defaultMeter(meter)
	cfg := newConfig(opts, trace.SpanKindInternal)

	durationHist, _ := m.Float64Histogram(
		MetricCommandDuration,
		metric.WithDescription("Measures the duration of command execution in seconds."),
		metric.WithUnit("s"),
	)

	counter, _ := m.Int64Counter(
		MetricCommandCount,
		metric.WithDescription("Counts the total number of executed commands."),
		metric.WithUnit("1"),
	)

	return func(ctx command.Context, cmd any, next func(command.Context, any) error) error {
		parentCtx := parentContextWithRemote(ctx, ctx.Instrumentation())
		cmdType := fmt.Sprintf("%T", cmd)

		spanOpts := []trace.SpanStartOption{
			trace.WithSpanKind(cfg.spanKind),
			trace.WithAttributes(attribute.String(AttrCommandType, cmdType)),
		}
		if actorID := ctx.Actor().Identifier.String(); actorID != "" {
			spanOpts = append(spanOpts, trace.WithAttributes(attribute.String(AttrActor, actorID)))
		}

		spanCtx, span := tr.Start(parentCtx, "flux.command", spanOpts...)
		defer span.End()

		inst := instrumentationFromSpan(span)
		childCtx := command.NewContext(
			spanCtx,
			ctx.CommandIdentifier(),
			ctx.Actor(),
			ctx.CorrelationIdentifier(),
			ctx.CausationIdentifier(),
			flux.WithInstrumentation(inst),
		)

		start := time.Now()
		err := next(childCtx, cmd)
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
			attribute.String(AttrCommandType, cmdType),
			attribute.String(AttrCommandStatus, status),
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
