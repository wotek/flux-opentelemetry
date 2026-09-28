package fluxotel

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type eventStoreWrapper struct {
	store                flux.EventStore
	tracer               trace.Tracer
	appendDuration       metric.Float64Histogram
	concurrencyConflicts metric.Int64Counter
	appendEvents         metric.Int64Counter
	readDuration         metric.Float64Histogram
}

var _ flux.EventStore = (*eventStoreWrapper)(nil)

// WrapEventStore wraps a flux.EventStore with OpenTelemetry tracing and metrics.
//
// Iterator Spans:
// Read and Stream operations implement two-phase span lifecycles. A short setup span
// traces the initial fetch and ends immediately when the iterator handle is returned.
// An iteration span begins when the caller ranges the iterator and ends when iteration
// completes, terminates early, or encounters an error. Callers should consume returned
// iterators; iteration cost is attributed to the iteration span.
func WrapEventStore(store flux.EventStore, tracer trace.Tracer, meter metric.Meter) flux.EventStore {
	tr := defaultTracer(tracer)
	m := defaultMeter(meter)

	appendDuration, _ := m.Float64Histogram(
		MetricEventStoreAppendDuration,
		metric.WithDescription("Measures the duration of event store append operations in seconds."),
		metric.WithUnit("s"),
	)

	concurrencyConflicts, _ := m.Int64Counter(
		MetricEventStoreAppendConflicts,
		metric.WithDescription("Counts optimistic concurrency conflicts during event store append operations."),
		metric.WithUnit("1"),
	)

	appendEvents, _ := m.Int64Counter(
		MetricEventStoreAppendEvents,
		metric.WithDescription("Counts the total number of events appended to the event store."),
		metric.WithUnit("1"),
	)

	readDuration, _ := m.Float64Histogram(
		MetricEventStoreReadDuration,
		metric.WithDescription("Measures the duration of event store read and stream operations in seconds."),
		metric.WithUnit("s"),
	)

	return &eventStoreWrapper{
		store:                store,
		tracer:               tr,
		appendDuration:       appendDuration,
		concurrencyConflicts: concurrencyConflicts,
		appendEvents:         appendEvents,
		readDuration:         readDuration,
	}
}

func (s *eventStoreWrapper) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
	spanCtx, span := s.tracer.Start(ctx, "flux.event_store.append",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(AttrStreamID, stream.Identifier.String()),
		),
	)
	defer span.End()

	start := time.Now()
	err := s.store.Append(spanCtx, stream, expectedRevision, events)
	duration := time.Since(start).Seconds()

	status := StatusOk
	if err != nil {
		status = StatusError
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if errors.Is(err, flux.ErrConcurrency) && s.concurrencyConflicts != nil {
			s.concurrencyConflicts.Add(spanCtx, 1)
		}
	} else {
		span.SetStatus(codes.Ok, "")
	}

	statusAttr := attribute.String(AttrEventStoreStatus, status)
	if s.appendDuration != nil {
		s.appendDuration.Record(spanCtx, duration, metric.WithAttributes(statusAttr))
	}
	if s.appendEvents != nil {
		s.appendEvents.Add(spanCtx, int64(len(events)), metric.WithAttributes(statusAttr))
	}

	return err
}

func (s *eventStoreWrapper) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
	setupCtx, setupSpan := s.tracer.Start(ctx, "flux.event_store.read",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(AttrStreamID, stream.Identifier.String()),
		),
	)

	startSetup := time.Now()
	iter, err := s.store.Read(setupCtx, stream, fromRevision)
	setupDuration := time.Since(startSetup).Seconds()
	if err != nil {
		setupSpan.RecordError(err)
		setupSpan.SetStatus(codes.Error, err.Error())
		setupSpan.End()
		if s.readDuration != nil {
			s.readDuration.Record(setupCtx, setupDuration, metric.WithAttributes(
				attribute.String(AttrEventStoreOp, "read"),
				attribute.String(AttrEventStoreStatus, StatusError),
			))
		}
		return nil, err
	}
	setupSpan.SetStatus(codes.Ok, "")
	setupSpan.End()

	return func(yield func(flux.Envelope, error) bool) {
		iterCtx, iterSpan := s.tracer.Start(ctx, "flux.event_store.read.iteration",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String(AttrStreamID, stream.Identifier.String()),
			),
		)
		startIter := time.Now()

		var once sync.Once
		endSpan := func(iterErr error) {
			once.Do(func() {
				iterDuration := time.Since(startIter).Seconds()
				status := StatusOk
				if iterErr != nil {
					status = StatusError
					iterSpan.RecordError(iterErr)
					iterSpan.SetStatus(codes.Error, iterErr.Error())
				} else {
					iterSpan.SetStatus(codes.Ok, "")
				}
				if s.readDuration != nil {
					s.readDuration.Record(iterCtx, iterDuration, metric.WithAttributes(
						attribute.String(AttrEventStoreOp, "read"),
						attribute.String(AttrEventStoreStatus, status),
					))
				}
				iterSpan.End()
			})
		}

		defer func() {
			endSpan(nil)
		}()
		for env, err := range iter {
			if err != nil {
				endSpan(err)
				yield(env, err)
				return
			}
			if !yield(env, nil) {
				return
			}
		}
	}, nil
}

func (s *eventStoreWrapper) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error) {
	setupCtx, setupSpan := s.tracer.Start(ctx, "flux.event_store.stream",
		trace.WithSpanKind(trace.SpanKindClient),
	)

	startSetup := time.Now()
	iter, err := s.store.Stream(setupCtx, position)
	setupDuration := time.Since(startSetup).Seconds()
	if err != nil {
		setupSpan.RecordError(err)
		setupSpan.SetStatus(codes.Error, err.Error())
		setupSpan.End()
		if s.readDuration != nil {
			s.readDuration.Record(setupCtx, setupDuration, metric.WithAttributes(
				attribute.String(AttrEventStoreOp, "stream"),
				attribute.String(AttrEventStoreStatus, StatusError),
			))
		}
		return nil, err
	}
	setupSpan.SetStatus(codes.Ok, "")
	setupSpan.End()

	return func(yield func(flux.Envelope, error) bool) {
		iterCtx, iterSpan := s.tracer.Start(ctx, "flux.event_store.stream.iteration",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		startIter := time.Now()

		var once sync.Once
		endSpan := func(iterErr error) {
			once.Do(func() {
				iterDuration := time.Since(startIter).Seconds()
				status := StatusOk
				if iterErr != nil {
					status = StatusError
					iterSpan.RecordError(iterErr)
					iterSpan.SetStatus(codes.Error, iterErr.Error())
				} else {
					iterSpan.SetStatus(codes.Ok, "")
				}
				if s.readDuration != nil {
					s.readDuration.Record(iterCtx, iterDuration, metric.WithAttributes(
						attribute.String(AttrEventStoreOp, "stream"),
						attribute.String(AttrEventStoreStatus, status),
					))
				}
				iterSpan.End()
			})
		}

		defer func() {
			endSpan(nil)
		}()
		for env, err := range iter {
			if err != nil {
				endSpan(err)
				yield(env, err)
				return
			}
			if !yield(env, nil) {
				return
			}
		}
	}, nil
}

type snapshotStoreWrapper[S any] struct {
	store        flux.SnapshotStore[S]
	tracer       trace.Tracer
	loadDuration metric.Float64Histogram
	saveDuration metric.Float64Histogram
	loadMisses   metric.Int64Counter
}

var _ flux.SnapshotStore[any] = (*snapshotStoreWrapper[any])(nil)

// WrapSnapshotStore wraps a flux.SnapshotStore with OpenTelemetry tracing and metrics.
func WrapSnapshotStore[S any](store flux.SnapshotStore[S], tracer trace.Tracer, meter metric.Meter) flux.SnapshotStore[S] {
	tr := defaultTracer(tracer)
	m := defaultMeter(meter)

	loadDuration, _ := m.Float64Histogram(
		MetricSnapshotLoadDuration,
		metric.WithDescription("Measures the duration of snapshot load operations in seconds."),
		metric.WithUnit("s"),
	)

	saveDuration, _ := m.Float64Histogram(
		MetricSnapshotSaveDuration,
		metric.WithDescription("Measures the duration of snapshot save operations in seconds."),
		metric.WithUnit("s"),
	)

	loadMisses, _ := m.Int64Counter(
		MetricSnapshotLoadMisses,
		metric.WithDescription("Counts snapshot load cache misses."),
		metric.WithUnit("1"),
	)

	return &snapshotStoreWrapper[S]{
		store:        store,
		tracer:       tr,
		loadDuration: loadDuration,
		saveDuration: saveDuration,
		loadMisses:   loadMisses,
	}
}

func (s *snapshotStoreWrapper[S]) Load(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error) {
	spanCtx, span := s.tracer.Start(ctx, "flux.snapshot_store.load",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(AttrStreamID, stream.Identifier.String()),
		),
	)
	defer span.End()

	start := time.Now()
	snap, err := s.store.Load(spanCtx, stream)
	duration := time.Since(start).Seconds()

	status := StatusOk
	if err != nil {
		if errors.Is(err, flux.ErrSnapshotNotFound) {
			if s.loadMisses != nil {
				s.loadMisses.Add(spanCtx, 1)
			}
			span.SetAttributes(attribute.Bool(AttrSnapshotMiss, true))
			span.SetStatus(codes.Ok, "")
		} else {
			status = StatusError
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	} else {
		span.SetStatus(codes.Ok, "")
	}

	if s.loadDuration != nil {
		s.loadDuration.Record(spanCtx, duration, metric.WithAttributes(
			attribute.String(AttrSnapshotStatus, status),
		))
	}

	return snap, err
}

func (s *snapshotStoreWrapper[S]) Save(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error {
	spanCtx, span := s.tracer.Start(ctx, "flux.snapshot_store.save",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(AttrStreamID, stream.Identifier.String()),
		),
	)
	defer span.End()

	start := time.Now()
	err := s.store.Save(spanCtx, stream, snap)
	duration := time.Since(start).Seconds()

	status := StatusOk
	if err != nil {
		status = StatusError
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	if s.saveDuration != nil {
		s.saveDuration.Record(spanCtx, duration, metric.WithAttributes(
			attribute.String(AttrSnapshotStatus, status),
		))
	}

	return err
}

type projectionStoreWrapper struct {
	store          projection.Store
	tracer         trace.Tracer
	updateDuration metric.Float64Histogram
}

var _ projection.Store = (*projectionStoreWrapper)(nil)

// WrapProjectionStore wraps a projection.Store with OpenTelemetry tracing and metrics.
func WrapProjectionStore(store projection.Store, tracer trace.Tracer, meter metric.Meter) projection.Store {
	tr := defaultTracer(tracer)
	m := defaultMeter(meter)

	updateDuration, _ := m.Float64Histogram(
		MetricProjectionStoreUpdateDuration,
		metric.WithDescription("Measures the duration of projection store update operations in seconds."),
		metric.WithUnit("s"),
	)

	return &projectionStoreWrapper{
		store:          store,
		tracer:         tr,
		updateDuration: updateDuration,
	}
}

func (p *projectionStoreWrapper) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	spanCtx, span := p.tracer.Start(ctx, "flux.projection_store.get_position",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String(AttrProjectionID, id.String())),
	)
	defer span.End()

	pos, err := p.store.GetPosition(spanCtx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return pos, err
}

func (p *projectionStoreWrapper) Update(ctx context.Context, id flux.Identifier, env flux.Envelope, mutate func(txCtx context.Context) error) error {
	spanCtx, span := p.tracer.Start(ctx, "flux.projection_store.update",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(AttrProjectionID, id.String()),
			attribute.String(AttrEventName, env.Event.Name()),
		),
	)
	defer span.End()

	start := time.Now()
	err := p.store.Update(spanCtx, id, env, mutate)
	duration := time.Since(start).Seconds()

	status := StatusOk
	if err != nil {
		status = StatusError
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	if p.updateDuration != nil {
		p.updateDuration.Record(spanCtx, duration, metric.WithAttributes(
			attribute.String(AttrProjectionStatus, status),
		))
	}

	return err
}
