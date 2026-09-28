package fluxotel

import (
	"fmt"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/query"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// QueryMiddleware returns a query bus middleware that traces query executions
// and records duration and throughput metrics.
//
// Remote Parenting:
// If the query context carries valid Instrumentation and the Go context does not
// already have an active span, the middleware reconstructs a remote SpanContext
// to ensure the query span seamlessly continues the originating distributed trace.
func QueryMiddleware(tracer trace.Tracer, meter metric.Meter, opts ...Option) query.Middleware {
	tr := defaultTracer(tracer)
	m := defaultMeter(meter)
	cfg := newConfig(opts, trace.SpanKindInternal)

	durationHist, _ := m.Float64Histogram(
		MetricQueryDuration,
		metric.WithDescription("Measures the duration of query execution in seconds."),
		metric.WithUnit("s"),
	)

	counter, _ := m.Int64Counter(
		MetricQueryCount,
		metric.WithDescription("Counts the total number of executed queries."),
		metric.WithUnit("1"),
	)

	return func(ctx query.Context, q any, next func(query.Context, any) (any, error)) (any, error) {
		parentCtx := parentContextWithRemote(ctx, ctx.Instrumentation())
		queryType := fmt.Sprintf("%T", q)

		spanOpts := []trace.SpanStartOption{
			trace.WithSpanKind(cfg.spanKind),
			trace.WithAttributes(attribute.String(AttrQueryType, queryType)),
		}
		if actorID := ctx.Actor().Identifier.String(); actorID != "" {
			spanOpts = append(spanOpts, trace.WithAttributes(attribute.String(AttrActor, actorID)))
		}

		spanCtx, span := tr.Start(parentCtx, "flux.query", spanOpts...)
		defer span.End()

		inst := instrumentationFromSpan(span)
		childCtx := query.NewContext(
			spanCtx,
			ctx.QueryIdentifier(),
			ctx.Actor(),
			ctx.CorrelationIdentifier(),
			ctx.CausationIdentifier(),
			flux.WithInstrumentation(inst),
		)

		start := time.Now()
		res, err := next(childCtx, q)
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
			attribute.String(AttrQueryType, queryType),
			attribute.String(AttrQueryStatus, status),
		)
		if durationHist != nil {
			durationHist.Record(spanCtx, duration, metricAttrs)
		}
		if counter != nil {
			counter.Add(spanCtx, 1, metricAttrs)
		}

		return res, err
	}
}
