//go:build linux && response_continuation_spike && cgo

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	native "github.com/inngest/inngest/internal/responsecontinuationspike"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/inngest/inngest/pkg/tracing/metadata"
	"github.com/inngest/inngest/pkg/util/interval"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// This reuses every literal and assertion at the original Go owner boundary.
// Only the decision owner changes; no fixture's Prepared field is consumed.
func TestResponseContinuationIntegratedContract(t *testing.T) {
	runResponseContinuationContract(t, func(_ *testing.T, ctx context.Context, e *executor, i *runInstance, resp *state.DriverResponse) error {
		return runNativeResponse(ctx, e, i, resp)
	})
}

// Whole-batch rejection is observed before metadata, history or any real port.
func TestResponseContinuationIntegratedRejectsWholeBatch(t *testing.T) {
	for _, excluded := range []*state.GeneratorOpcode{nil, {Op: enums.OpcodeSleep, ID: "sleep", Name: "1s"}, {Op: enums.OpcodeStepError, ID: "invalid"}, {Op: enums.Opcode(255)}} {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "eligible"}, excluded)
		i.md.Config.RequestVersion = -1
		require.Error(t, runNativeResponse(context.Background(), e, i, i.resp))
		require.Empty(t, events.snapshot())
		require.Empty(t, s.writes)
		require.Empty(t, q.calls)
	}
}

func TestResponseContinuationIntegratedSiblingProgressAndJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "a"}, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "b"})
		entered := make(chan string, 2)
		release := make(chan struct{})
		defer close(release)
		s.beforeSave = func(id string) {
			entered <- id
			if id == "b" {
				<-release
			}
		}
		original := errors.New("b save rejected")
		s.outcomes["b"] = stagedSaveOutcome{err: original}
		result := make(chan error, 1)
		go func() { result <- runNativeResponse(context.Background(), e, i, i.resp) }()
		require.ElementsMatch(t, []string{"a", "b"}, []string{<-entered, <-entered})
		synctest.Wait()
		require.Len(t, q.calls, 1, "a reaches enqueue while b's SaveStep is outstanding")
		require.Contains(t, events.snapshot(), "scheduled:edge:<nil>:0")
		select {
		case err := <-result:
			t.Fatalf("returned before sibling join: %v", err)
		default:
		}
		release <- struct{}{}
		require.Same(t, original, <-result)
	})
}

type nativeSavePort struct {
	sv2.RunService
	save func(context.Context, sv2.ID, string, []byte) (bool, error)
}

func (s *nativeSavePort) SaveStep(ctx context.Context, id sv2.ID, step string, output []byte) (bool, error) {
	return s.save(ctx, id, step, output)
}

// Explicit close must cancel the actual effect context, join a port that is
// still returning, and then close native state. No callbacks/next effects run.
func TestResponseContinuationIntegratedCloseOutstandingSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"})
		entered := make(chan struct{})
		cancelSeen := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		calls := atomic.Int32{}
		e.smv2 = &nativeSavePort{RunService: s, save: func(ctx context.Context, _ sv2.ID, _ string, _ []byte) (bool, error) {
			calls.Add(1)
			close(entered)
			<-ctx.Done()
			close(cancelSeen)
			<-release
			return false, ctx.Err()
		}}
		d, err := newContinuationDriver(context.Background(), e, i, i.resp)
		require.NoError(t, err)
		result := make(chan error, 1)
		go func() { result <- d.run() }()
		<-entered
		closed := make(chan struct{})
		go func() { d.Close(); close(closed) }()
		<-cancelSeen
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("close released an outstanding Go effect")
		default:
		}
		release <- struct{}{}
		<-closed
		require.ErrorIs(t, <-result, context.Canceled)
		require.Equal(t, int32(1), calls.Load())
		require.Empty(t, q.calls)
		require.Empty(t, events.snapshot())
		_, err = d.session.Resume(native.Key{}, "Done")
		require.ErrorContains(t, err, "closed")
	})
}

type nativeBlockedListener struct {
	execution.LifecycleListener
	entered   chan struct{}
	release   chan struct{}
	cancelled chan struct{}
	value     any
	item      queue.Item
}

func (l *nativeBlockedListener) OnStepScheduled(ctx context.Context, _ sv2.Metadata, item queue.Item, _ *string) {
	l.item = item
	l.value = ctx.Value(nativeContextKey{})
	close(l.entered)
	<-ctx.Done()
	close(l.cancelled)
	<-l.release
}

type nativeContextKey struct{}

type nativePanicSync struct {
	execution.NoopSyncLifecycleListener
}

func (*nativePanicSync) OnStepScheduled(context.Context, sv2.Metadata, queue.Item, *string, time.Time) {
	panic("sync listener isolated panic")
}

func TestResponseContinuationIntegratedCloseDrainsCallbacksSeparately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, _, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "p", Name: "planned"})
		e.tracerProvider = tracing.NewOtelTracerProvider(&continuationExporter{q: q, events: events}, 0)
		listener := &nativeBlockedListener{entered: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
		defer close(listener.release)
		e.lifecycles = []execution.LifecycleListener{listener}
		e.syncLifecycles = append([]execution.SyncLifecycleListener{&nativePanicSync{}}, e.syncLifecycles...)
		value := &struct{ Label string }{"original-context"}
		d, err := newContinuationDriver(context.WithValue(context.Background(), nativeContextKey{}, value), e, i, i.resp)
		require.NoError(t, err)
		result := make(chan error, 1)
		go func() { result <- d.run() }()
		<-listener.entered
		// Response completion acknowledges launch, not delivery. Explicit close
		// cancels the actual listener context and separately joins the callback.
		require.NoError(t, <-result)
		require.Contains(t, events.snapshot(), "scheduled:edge:planned:0", "sync panic does not suppress later listeners")
		closed := make(chan struct{})
		go func() { _ = d.Close(); close(closed) }()
		<-listener.cancelled
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("closed before async listener drain")
		default:
		}
		require.Same(t, value, listener.value)
		require.Equal(t, q.calls[0].item.GroupID, listener.item.GroupID)
		require.NotEmpty(t, q.calls[0].item.Metadata[meta.PropagationKey])
		require.Equal(t, q.calls[0].item.Metadata, listener.item.Metadata)
		listener.release <- struct{}{}
		<-closed
	})
}

func TestResponseContinuationIntegratedRecoveredPanicJoinsSibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, _ := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "panics"}, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "sibling"})
		entered := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		e.smv2 = &nativeSavePort{RunService: s, save: func(ctx context.Context, id sv2.ID, step string, output []byte) (bool, error) {
			if step == "panics" {
				panic("recovered Go effect panic")
			}
			close(entered)
			<-release
			return s.SaveStep(ctx, id, step, output)
		}}
		result := make(chan error, 1)
		go func() { result <- runNativeResponse(context.Background(), e, i, i.resp) }()
		<-entered
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("panic skipped sibling join: %v", err)
		default:
		}
		release <- struct{}{}
		require.NoError(t, <-result)
		require.Equal(t, map[string]string{"sibling": `{"data":null}`}, s.writes)
		require.Len(t, q.calls, 1)
	})
}

// A protocol failure after the real store committed poisons this session. The
// same session cannot retry/resume/fallback and cannot issue lifecycle/queue work.
func TestResponseContinuationIntegratedPoisonAfterCommittedSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "blocked"})
		original := errors.New("commit acknowledged with error")
		entered := make(chan string, 2)
		releaseSave := make(chan struct{})
		releaseSibling := make(chan struct{})
		cancelSeen := make(chan struct{})
		defer close(releaseSave)
		defer close(releaseSibling)
		calls := atomic.Int32{}
		e.smv2 = &nativeSavePort{RunService: s, save: func(ctx context.Context, id sv2.ID, step string, out []byte) (bool, error) {
			calls.Add(1)
			entered <- step
			if step == "blocked" {
				<-ctx.Done()
				close(cancelSeen)
				<-releaseSibling
				return false, ctx.Err()
			}
			<-releaseSave
			pending, err := s.SaveStep(ctx, id, step, out)
			if err != nil {
				return pending, err
			}
			return pending, original
		}}
		d, err := newContinuationDriver(context.Background(), e, i, i.resp)
		require.NoError(t, err)
		resume := d.resume
		d.resume = func(key native.Key, observation any) (native.Progress, error) {
			if value, ok := observation.(map[string]any); ok && value["Saved"] != nil {
				key.Generation++
			}
			return resume(key, observation)
		}
		result := make(chan error, 1)
		go func() { result <- d.run() }()
		require.ElementsMatch(t, []string{"s", "blocked"}, []string{<-entered, <-entered})
		releaseSave <- struct{}{}
		<-cancelSeen
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("poison did not join outstanding sibling: %v", err)
		default:
		}
		releaseSibling <- struct{}{}
		require.ErrorContains(t, <-result, "poisoned")
		require.NoError(t, d.drain())
		require.Equal(t, int32(2), calls.Load())
		require.Equal(t, map[string]string{"s": `{"data":null}`}, s.writes)
		require.Empty(t, q.calls)
		require.Equal(t, []string{"metadata_update", "handler:save:s"}, events.snapshot())
		require.Same(t, original, d.originalError(3))
		_, err = d.session.Resume(native.Key{}, "Done")
		require.Error(t, err)
		require.Equal(t, int32(2), calls.Load())
	})
}

func TestResponseContinuationIntegratedRetainsFullDescriptor(t *testing.T) {
	g := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s", Name: "sdk-name", Metadata: []metadata.ScopedUpdate{stepMetadataUpdate("userland.custom", metadata.Values{"k": json.RawMessage(`"v"`)})}}
	g.Timing = interval.Interval{A: 1793016000000000000, B: 42_000_000}
	g.Userland = &struct {
		ID    string `json:"id"`
		Index int    `json:"index,omitempty"`
	}{ID: "user-id", Index: 9}
	e, i, s, q, _ := newStagedBatch(t, g)
	i.md.Config.StartedAt = time.Unix(0, 0).UTC()
	i.md.Config.RequestVersion = -1
	boundTime := time.Date(2026, 10, 26, 12, 0, 0, 0, time.FixedZone("bound-clock", 0))
	e.clock = clockwork.NewFakeClockAt(boundTime)
	times := &nativeTimeListener{}
	e.syncLifecycles = append(e.syncLifecycles, times)
	i.md.Metrics.MetadataSize = 11
	i.md.Metrics.MetadataSizeLoaded = 7
	tracer := newRecordingTracerProvider()
	e.tracerProvider = tracer
	require.NoError(t, runNativeResponse(context.Background(), e, i, i.resp))
	require.Equal(t, map[string]int{"s": 8}, s.saveDeltas)
	require.Equal(t, boundTime, q.calls[0].at)
	require.Equal(t, boundTime, times.lifecycle)
	require.Equal(t, boundTime, times.scheduled)
	_, updates, _, _ := s.snapshot()
	require.Equal(t, i.md.Config.StartedAt, updates[0].StartedAt)
	found := false
	for _, call := range tracer.createCalls {
		if call.name == meta.SpanNameStep {
			found = true
			started, ok := meta.GetAttr(call.opts.Attributes, meta.Attrs.StartedAt)
			require.True(t, ok)
			require.True(t, g.Timing.Start().Equal(*started))
			id, ok := meta.GetAttr(call.opts.Attributes, meta.Attrs.StepUserlandID)
			require.True(t, ok)
			require.Equal(t, "user-id", *id)
		}
	}
	require.True(t, found)
}

// Activation is a real metric/span effect. None still counts, and permanent
// StepError conversion must leave the incoming execution span in Error state.
func TestResponseContinuationIntegratedActivation(t *testing.T) {
	// The metrics registry caches instruments globally. Isolate its real provider
	// lifecycle so this contract also works after other tests or with -count.
	const marker = "RESPONSE_NATIVE_ACTIVATION_CHILD"
	if os.Getenv(marker) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestResponseContinuationIntegratedActivation$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), marker+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		return
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer otel.SetMeterProvider(previous)
	defer provider.Shutdown(context.Background())
	var before metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &before))
	e, i, _, _, _ := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeNone, ID: "none"}, &state.GeneratorOpcode{Op: enums.OpcodeStepError, ID: "failed", Error: &state.UserError{Name: "Permanent", NoRetry: true}})
	traceProvider := sdktrace.NewTracerProvider()
	defer traceProvider.Shutdown(context.Background())
	ctx, incoming := traceProvider.Tracer("activation-contract").Start(context.Background(), "incoming")
	require.NoError(t, runNativeResponse(ctx, e, i, i.resp))
	incoming.End()
	snapshot := incoming.(sdktrace.ReadOnlySpan)
	require.Equal(t, codes.Error, snapshot.Status().Code)
	require.Equal(t, "Permanent", snapshot.Status().Description)
	var after metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &after))
	counts := func(metrics metricdata.ResourceMetrics) map[string]int64 {
		result := map[string]int64{}
		for _, scope := range metrics.ScopeMetrics {
			for _, metric := range scope.Metrics {
				if metric.Name == "inngest_executor_handle_generator_total" {
					sum, ok := metric.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, point := range sum.DataPoints {
						op, ok := point.Attributes.Value(attribute.Key("op"))
						require.True(t, ok)
						result[op.AsString()] += point.Value
					}
				}
			}
		}
		return result
	}
	a, b := counts(after), counts(before)
	require.Equal(t, int64(1), a["None"]-b["None"])
	require.Equal(t, int64(1), a["StepError"]-b["StepError"])
	require.Equal(t, int64(0), a["StepFailed"]-b["StepFailed"], "activation precedes local conversion")
}

// A process boundary is required to observe Go's unrecovered async panic. A
// sync listener panic above remains recovered and later listeners still run.
func TestResponseContinuationIntegratedAsyncPanicSemantics(t *testing.T) {
	const marker = "RESPONSE_NATIVE_ASYNC_PANIC_CHILD"
	if os.Getenv(marker) == "1" {
		e, i, _, _, _ := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "p"})
		e.lifecycles = []execution.LifecycleListener{&nativePanicAsync{}}
		_ = runNativeResponse(context.Background(), e, i, i.resp)
		t.Fatal("async listener panic was suppressed")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestResponseContinuationIntegratedAsyncPanicSemantics$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), marker+"=1")
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "panic: async-listener-must-remain-unrecovered")
}

type nativePanicAsync struct{ execution.LifecycleListener }

func (*nativePanicAsync) OnStepScheduled(context.Context, sv2.Metadata, queue.Item, *string) {
	panic("async-listener-must-remain-unrecovered")
}

type nativeTimeListener struct {
	execution.NoopSyncLifecycleListener
	lifecycle, scheduled time.Time
}

func (l *nativeTimeListener) OnStepRunFinished(_ context.Context, _ sv2.Metadata, _ queue.Item, _ inngest.Edge, _ state.GeneratorOpcode, at time.Time) {
	l.lifecycle = at
}
func (l *nativeTimeListener) OnStepScheduled(_ context.Context, _ sv2.Metadata, _ queue.Item, _ *string, at time.Time) {
	l.scheduled = at
}

// The shared literals cannot observe runInstance's private capacity callback.
// Reuse the existing RunContext observation wrapper to guard the native release
// request without exporting or injecting anything into the production queue.
func TestResponseContinuationIntegratedReleasePort(t *testing.T) {
	for _, op := range []enums.Opcode{enums.OpcodeStepRun, enums.OpcodeStepFailed} {
		for _, saveErr := range []error{nil, context.Canceled} {
			t.Run(fmt.Sprintf("%s/rejected=%t", op, saveErr != nil), func(t *testing.T) {
				e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: op, ID: "s", Error: &state.UserError{Name: "E", Message: "x"}})
				s.outcomes["s"] = stagedSaveOutcome{err: saveErr}
				d, err := newContinuationDriver(context.Background(), e, i, i.resp)
				require.NoError(t, err)
				for index := range d.branches {
					b := &d.branches[index]
					b.step.runCtx = &stagedReleaseContext{runInstance: &b.instance, events: events}
				}
				result := d.run()
				require.NoError(t, d.drain())
				if saveErr != nil {
					require.Same(t, saveErr, result)
					require.Equal(t, []string{"handler:save:s"}, events.snapshot())
					require.Empty(t, q.calls)
				} else {
					require.NoError(t, result)
					observed := events.snapshot()
					require.Len(t, observed, 5)
					require.Equal(t, "handler:save:s", observed[0])
					require.Equal(t, "release", observed[2])
					require.Equal(t, "handler:enqueue:s", observed[3])
				}
			})
		}
	}
}

func TestResponseContinuationIntegratedCloseDispatchAdmission(t *testing.T) {
	for _, shutdown := range []string{"Close", "ExternalContextCancellation"} {
		t.Run(shutdown, func(t *testing.T) {
			e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{
				Op: enums.OpcodeStepError, ID: "s", Error: &state.UserError{Name: "Permanent", NoRetry: true},
			})
			traceProvider := sdktrace.NewTracerProvider()
			defer traceProvider.Shutdown(context.Background())
			parentCtx, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			ctx, incoming := traceProvider.Tracer("close-dispatch-admission").Start(parentCtx, "incoming")
			d, err := newContinuationDriver(ctx, e, i, i.resp)
			require.NoError(t, err)

			blocked := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseDispatch := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() {
				releaseDispatch()
				_ = d.Close()
			}()
			d.beforeAdmission = func(request native.Request) {
				kind, _, err := nativeVariant(request.Effect)
				if err == nil && kind == "ActivateBranch" {
					close(blocked)
					<-release
				}
			}

			runResult := make(chan error, 1)
			go func() { runResult <- d.run() }()
			select {
			case <-blocked:
			case <-time.After(10 * time.Second):
				t.Fatal("runner did not reach the activation admission window")
			}

			var closeResult chan error
			if shutdown == "Close" {
				closeResult = make(chan error, 1)
				go func() { closeResult <- d.Close() }()
				select {
				case <-d.ctx.Done():
				case <-time.After(10 * time.Second):
					t.Fatal("close did not cancel the driver while dispatch was paused")
				}
			} else {
				cancelParent()
				select {
				case <-d.watcherDone:
				case <-time.After(10 * time.Second):
					t.Fatal("external cancellation watcher did not close dispatch admission")
				}
			}
			releaseDispatch()

			require.ErrorIs(t, <-runResult, context.Canceled)
			if closeResult != nil {
				require.NoError(t, <-closeResult)
			} else {
				require.NoError(t, d.drain())
			}
			select {
			case <-d.watcherDone:
			default:
				t.Fatal("driver drain returned before joining the context watcher")
			}
			incoming.End()
			require.Equal(t, codes.Unset, incoming.(sdktrace.ReadOnlySpan).Status().Code,
				"a ready activation paused before admission must not run after shutdown")
			require.Empty(t, s.writes)
			require.Empty(t, q.calls)
			require.Empty(t, events.snapshot())
		})
	}
}

func TestResponseContinuationIntegratedCloseBeforeDispatch(t *testing.T) {
	e, i, _, _, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"})
	d, err := newContinuationDriver(context.Background(), e, i, i.resp)
	require.NoError(t, err)
	require.NoError(t, d.Close())
	require.NoError(t, d.Close())
	require.ErrorContains(t, d.run(), "already started or closed")
	require.Empty(t, events.snapshot())
}
