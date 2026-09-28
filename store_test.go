package fluxotel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wotek/flux"
	fluxotel "github.com/wotek/flux-opentelemetry"
	eventstore "github.com/wotek/flux/event/store"
	projstore "github.com/wotek/flux/projection/store"
	snapstore "github.com/wotek/flux/snapshot/store"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type dummyEvent struct {
	Msg string
}

func (d dummyEvent) Name() string { return "dummy.event" }

type mockEventStore struct {
	appendFunc func(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error
	readFunc   func(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error)
	streamFunc func(ctx context.Context, position uint64) (flux.StreamIterator, error)
}

func (m *mockEventStore) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
	if m.appendFunc != nil {
		return m.appendFunc(ctx, stream, expectedRevision, events)
	}
	return nil
}

func (m *mockEventStore) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
	if m.readFunc != nil {
		return m.readFunc(ctx, stream, fromRevision)
	}
	return func(yield func(flux.Envelope, error) bool) {}, nil
}

func (m *mockEventStore) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error) {
	if m.streamFunc != nil {
		return m.streamFunc(ctx, position)
	}
	return func(yield func(flux.Envelope, error) bool) {}, nil
}

type mockSnapshotStore[S any] struct {
	loadFunc func(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error)
	saveFunc func(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error
}

func (m *mockSnapshotStore[S]) Load(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error) {
	if m.loadFunc != nil {
		return m.loadFunc(ctx, stream)
	}
	return flux.Snapshot[S]{}, nil
}

func (m *mockSnapshotStore[S]) Save(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error {
	if m.saveFunc != nil {
		return m.saveFunc(ctx, stream, snap)
	}
	return nil
}

func TestWrapEventStore(t *testing.T) {
	t.Parallel()

	t.Run("Append success", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, reader, mp := setupTelemetry()
		baseStore := eventstore.New()
		store := fluxotel.WrapEventStore(baseStore, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		envelope := flux.Envelope{
			Identifier: flux.NewIdentifier("test", "", "test", "", "event", "1", ""),
			Event:      dummyEvent{Msg: "test"},
			Revision:   1,
		}

		err := store.Append(context.Background(), stream, 0, []flux.Envelope{envelope})
		if err != nil {
			t.Fatalf("unexpected append error: %v", err)
		}

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}
		if spans[0].Name != "flux.event_store.append" {
			t.Errorf("span.Name = %q, want %q", spans[0].Name, "flux.event_store.append")
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("failed to collect metrics: %v", err)
		}

		foundDuration := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == fluxotel.MetricEventStoreAppendDuration {
					foundDuration = true
				}
			}
		}
		if !foundDuration {
			t.Errorf("metric %s not found", fluxotel.MetricEventStoreAppendDuration)
		}
	})

	t.Run("Append concurrency conflict", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, reader, mp := setupTelemetry()
		mock := &mockEventStore{
			appendFunc: func(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
				return flux.ErrConcurrency
			},
		}
		store := fluxotel.WrapEventStore(mock, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		err := store.Append(context.Background(), stream, 1, []flux.Envelope{})
		if !errors.Is(err, flux.ErrConcurrency) {
			t.Fatalf("expected ErrConcurrency, got %v", err)
		}

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}
		if spans[0].Status.Code != codes.Error {
			t.Errorf("span status code = %v, want Error", spans[0].Status.Code)
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("failed to collect metrics: %v", err)
		}

		foundConflict := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == fluxotel.MetricEventStoreAppendConflicts {
					foundConflict = true
				}
			}
		}
		if !foundConflict {
			t.Errorf("metric %s not found", fluxotel.MetricEventStoreAppendConflicts)
		}
	})

	t.Run("Read and Stream iterators two-phase spans", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, _, mp := setupTelemetry()
		baseStore := eventstore.New()
		store := fluxotel.WrapEventStore(baseStore, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		envelope := flux.Envelope{
			Identifier: flux.NewIdentifier("test", "", "test", "", "event", "1", ""),
			Event:      dummyEvent{Msg: "test"},
			Revision:   1,
		}
		_ = store.Append(context.Background(), stream, 0, []flux.Envelope{envelope})

		// Test Read iteration
		iter, err := store.Read(context.Background(), stream, 0)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}

		readCount := 0
		for env, err := range iter {
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if env.Event.Name() == "dummy.event" {
				readCount++
			}
		}
		if readCount != 1 {
			t.Errorf("readCount = %d, want 1", readCount)
		}

		// Test Stream iteration
		streamIter, err := store.Stream(context.Background(), 0)
		if err != nil {
			t.Fatalf("unexpected stream error: %v", err)
		}

		streamCount := 0
		for env, err := range streamIter {
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if env.Event.Name() == "dummy.event" {
				streamCount++
			}
		}
		if streamCount != 1 {
			t.Errorf("streamCount = %d, want 1", streamCount)
		}

		spans := exporter.GetSpans()
		// 1 append, 1 read setup + 1 read iteration, 1 stream setup + 1 stream iteration = 5 spans
		if len(spans) != 5 {
			t.Fatalf("expected 5 spans, got %d", len(spans))
		}

		names := make(map[string]int)
		for _, s := range spans {
			names[s.Name]++
		}
		if names["flux.event_store.append"] != 1 {
			t.Errorf("append spans = %d, want 1", names["flux.event_store.append"])
		}
		if names["flux.event_store.read"] != 1 {
			t.Errorf("read setup spans = %d, want 1", names["flux.event_store.read"])
		}
		if names["flux.event_store.read.iteration"] != 1 {
			t.Errorf("read iteration spans = %d, want 1", names["flux.event_store.read.iteration"])
		}
		if names["flux.event_store.stream"] != 1 {
			t.Errorf("stream setup spans = %d, want 1", names["flux.event_store.stream"])
		}
		if names["flux.event_store.stream.iteration"] != 1 {
			t.Errorf("stream iteration spans = %d, want 1", names["flux.event_store.stream.iteration"])
		}
	})

	t.Run("Read unconsumed iterator does not leak span", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, _, mp := setupTelemetry()
		baseStore := eventstore.New()
		store := fluxotel.WrapEventStore(baseStore, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		_, err := store.Read(context.Background(), stream, 0)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}

		// Only setup span should exist and have ended; no iteration span started
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}
		if spans[0].Name != "flux.event_store.read" {
			t.Errorf("span.Name = %q, want %q", spans[0].Name, "flux.event_store.read")
		}
	})

	t.Run("Read mid-iteration error", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, _, mp := setupTelemetry()
		iterErr := errors.New("mid-stream failure")
		mock := &mockEventStore{
			readFunc: func(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
				return func(yield func(flux.Envelope, error) bool) {
					yield(flux.Envelope{}, iterErr)
				}, nil
			},
		}
		store := fluxotel.WrapEventStore(mock, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		iter, err := store.Read(context.Background(), stream, 0)
		if err != nil {
			t.Fatalf("unexpected read setup error: %v", err)
		}

		var receivedErr error
		for _, err := range iter {
			if err != nil {
				receivedErr = err
				break
			}
		}
		if !errors.Is(receivedErr, iterErr) {
			t.Fatalf("expected iterator error %v, got %v", iterErr, receivedErr)
		}

		spans := exporter.GetSpans()
		if len(spans) != 2 {
			t.Fatalf("expected 2 spans (setup + iteration), got %d", len(spans))
		}

		var iterSpan *tracetest.SpanStub
		for i := range spans {
			if spans[i].Name == "flux.event_store.read.iteration" {
				iterSpan = &spans[i]
			}
		}
		if iterSpan == nil {
			t.Fatal("iteration span not found")
		}
		if iterSpan.Status.Code != codes.Error {
			t.Errorf("iteration span status = %v, want %v", iterSpan.Status.Code, codes.Error)
		}
	})

	t.Run("Read error handling", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, _, mp := setupTelemetry()
		readErr := errors.New("read failed")
		mock := &mockEventStore{
			readFunc: func(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
				return nil, readErr
			},
		}
		store := fluxotel.WrapEventStore(mock, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		_, err := store.Read(context.Background(), stream, 0)
		if !errors.Is(err, readErr) {
			t.Fatalf("expected read error, got %v", err)
		}

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}
		if spans[0].Status.Code != codes.Error {
			t.Errorf("expected span error status, got %v", spans[0].Status.Code)
		}
	})
}

func TestWrapSnapshotStore(t *testing.T) {
	t.Parallel()

	t.Run("Save and Load success", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, reader, mp := setupTelemetry()
		baseStore := snapstore.New[string]()
		store := fluxotel.WrapSnapshotStore(baseStore, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		snap := flux.Snapshot[string]{State: "snapshot-data", Revision: 5}

		if err := store.Save(context.Background(), stream, snap); err != nil {
			t.Fatalf("unexpected save error: %v", err)
		}

		loaded, err := store.Load(context.Background(), stream)
		if err != nil {
			t.Fatalf("unexpected load error: %v", err)
		}
		if loaded.State != "snapshot-data" {
			t.Errorf("loaded state = %q, want %q", loaded.State, "snapshot-data")
		}

		spans := exporter.GetSpans()
		if len(spans) != 2 {
			t.Fatalf("expected 2 spans (save and load), got %d", len(spans))
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("failed to collect metrics: %v", err)
		}

		foundLoadDuration := false
		foundSaveDuration := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == fluxotel.MetricSnapshotLoadDuration {
					foundLoadDuration = true
				}
				if m.Name == fluxotel.MetricSnapshotSaveDuration {
					foundSaveDuration = true
				}
			}
		}
		if !foundLoadDuration {
			t.Errorf("metric %s not found", fluxotel.MetricSnapshotLoadDuration)
		}
		if !foundSaveDuration {
			t.Errorf("metric %s not found", fluxotel.MetricSnapshotSaveDuration)
		}
	})

	t.Run("Load miss counter and span status", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, reader, mp := setupTelemetry()
		baseStore := snapstore.New[string]()
		store := fluxotel.WrapSnapshotStore(baseStore, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "missing", "")}
		_, err := store.Load(context.Background(), stream)
		if !errors.Is(err, flux.ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
		}

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}

		// Snapshot miss must NOT be an Error on span
		if spans[0].Status.Code == codes.Error {
			t.Errorf("snapshot miss set span status to Error, want Ok or Unset")
		}

		// Must have flux.snapshot.miss attribute set to true
		hasMissAttr := false
		for _, attr := range spans[0].Attributes {
			if attr.Key == fluxotel.AttrSnapshotMiss && attr.Value.AsBool() {
				hasMissAttr = true
			}
		}
		if !hasMissAttr {
			t.Errorf("span missing %s=true attribute", fluxotel.AttrSnapshotMiss)
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("failed to collect metrics: %v", err)
		}

		foundMisses := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == fluxotel.MetricSnapshotLoadMisses {
					foundMisses = true
				}
			}
		}
		if !foundMisses {
			t.Errorf("metric %s not found", fluxotel.MetricSnapshotLoadMisses)
		}
	})

	t.Run("Load real error sets Error status", func(t *testing.T) {
		t.Parallel()
		exporter, tracer, _, mp := setupTelemetry()
		dbErr := errors.New("db connection failure")
		mock := &mockSnapshotStore[string]{
			loadFunc: func(ctx context.Context, stream flux.Stream) (flux.Snapshot[string], error) {
				return flux.Snapshot[string]{}, dbErr
			},
		}
		store := fluxotel.WrapSnapshotStore[string](mock, tracer, mp.Meter("test"))

		stream := flux.Stream{Identifier: flux.NewIdentifier("test", "", "test", "", "stream", "1", "")}
		_, err := store.Load(context.Background(), stream)
		if !errors.Is(err, dbErr) {
			t.Fatalf("expected dbErr, got %v", err)
		}

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}
		if spans[0].Status.Code != codes.Error {
			t.Errorf("real load error status = %v, want %v", spans[0].Status.Code, codes.Error)
		}
	})
}

func TestWrapProjectionStore(t *testing.T) {
	t.Parallel()

	exporter, tracer, reader, mp := setupTelemetry()
	baseStore := projstore.New()
	store := fluxotel.WrapProjectionStore(baseStore, tracer, mp.Meter("test"))

	projID := flux.NewIdentifier("test", "", "test", "", "proj", "1", "")
	env := flux.Envelope{
		Identifier: flux.NewIdentifier("test", "", "test", "", "event", "1", ""),
		Event:      dummyEvent{Msg: "proj-event"},
		Position:   10,
	}

	mutateCalled := false
	err := store.Update(context.Background(), projID, env, func(txCtx context.Context) error {
		mutateCalled = true
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}
	if !mutateCalled {
		t.Errorf("expected mutate to be called")
	}

	pos, err := store.GetPosition(context.Background(), projID)
	if err != nil {
		t.Fatalf("unexpected get position error: %v", err)
	}
	if pos != 10 {
		t.Errorf("position = %d, want 10", pos)
	}

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans (update and get_position), got %d", len(spans))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	foundUpdateDuration := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == fluxotel.MetricProjectionStoreUpdateDuration {
				foundUpdateDuration = true
			}
		}
	}
	if !foundUpdateDuration {
		t.Errorf("metric %s not found", fluxotel.MetricProjectionStoreUpdateDuration)
	}
}
