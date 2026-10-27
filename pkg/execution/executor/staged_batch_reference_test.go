package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/inngest/inngest/pkg/tracing/metadata"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// This is an observation adapter, not an intention planner. Effect outcomes are
// supplied independently of the literal expected port values below. The shared
// opcode-plan fixture remains the primary grouping/index/hash oracle.
type stagedSaveOutcome struct {
	pending bool
	err     error
}

// Actual port arguments and return values, observed only when SaveStep runs.
type stagedSaveObservation struct {
	runID          sv2.ID
	stepID, output string
	metadataDelta  int
	result         stagedSaveOutcome
}
type stagedBatchStore struct {
	*opcodePlanRecordingRunService
	mu            sync.Mutex
	writes        map[string]string
	saveDeltas    map[string]int
	outcomes      map[string]stagedSaveOutcome
	saves         []stagedSaveObservation
	metadataErr   error
	afterMetadata func()
	beforeSave    func(string)
}

func (s *stagedBatchStore) UpdateMetadata(ctx context.Context, id sv2.ID, update sv2.MutableConfig) error {
	_ = s.opcodePlanRecordingRunService.UpdateMetadata(ctx, id, update)
	if s.afterMetadata != nil {
		s.afterMetadata()
	}
	return s.metadataErr
}
func (s *stagedBatchStore) SaveStep(ctx context.Context, runID sv2.ID, id string, output []byte) (bool, error) {
	if s.beforeSave != nil {
		s.beforeSave(id)
	}
	s.mu.Lock()
	s.writes[id] = string(output)
	s.saveDeltas[id] = state.MetadataSizeDeltaFromContext(ctx)
	result := s.outcomes[id]
	s.saves = append(s.saves, stagedSaveObservation{runID: runID, stepID: id, output: string(output), metadataDelta: state.MetadataSizeDeltaFromContext(ctx), result: result})
	s.mu.Unlock()
	s.events.record("handler:save:" + id)
	return result.pending, result.err
}

type stagedEnqueue struct {
	item         queue.Item
	at           time.Time
	opts         queue.EnqueueOpts
	contextGroup string
}
type stagedBatchQueue struct {
	queue.Queue
	mu     sync.Mutex
	calls  []stagedEnqueue
	events *opcodePlanEvents
	err    error
}

func (q *stagedBatchQueue) Enqueue(ctx context.Context, item queue.Item, at time.Time, opts queue.EnqueueOpts) error {
	q.mu.Lock()
	q.calls = append(q.calls, stagedEnqueue{item: item, at: at, opts: opts, contextGroup: state.GroupIDFromContext(ctx)})
	q.mu.Unlock()
	q.events.record("handler:enqueue:" + item.Payload.(queue.PayloadEdge).Edge.Outgoing)
	return q.err
}

type stagedBatchLifecycle struct {
	execution.NoopSyncLifecycleListener
	events *opcodePlanEvents
}

func (l *stagedBatchLifecycle) OnStepRunFinished(_ context.Context, _ sv2.Metadata, _ queue.Item, _ inngest.Edge, g state.GeneratorOpcode, _ time.Time) {
	l.events.record("finished:" + g.ID)
}
func (l *stagedBatchLifecycle) OnStepRunFailed(_ context.Context, _ sv2.Metadata, _ queue.Item, _ inngest.Edge, g state.GeneratorOpcode, status enums.StepStatus, attempt int, _ time.Time) {
	l.events.record(fmt.Sprintf("failed:%s:%s:%d", g.ID, status, attempt))
}
func (l *stagedBatchLifecycle) OnStepScheduled(_ context.Context, _ sv2.Metadata, item queue.Item, name *string, _ time.Time) {
	n := "<nil>"
	if name != nil {
		n = *name
	}
	l.events.record(fmt.Sprintf("scheduled:%s:%s:%d", item.Kind, n, item.Attempt))
}
func (l *stagedBatchLifecycle) OnSleep(_ context.Context, _ sv2.Metadata, _ queue.Item, g state.GeneratorOpcode, _ time.Time, _ time.Time) {
	l.events.record("sleep:" + g.ID)
}

func newStagedBatch(t *testing.T, ops ...*state.GeneratorOpcode) (*executor, *runInstance, *stagedBatchStore, *stagedBatchQueue, *opcodePlanEvents) {
	t.Helper()
	events := &opcodePlanEvents{}
	store := &stagedBatchStore{opcodePlanRecordingRunService: &opcodePlanRecordingRunService{events: events}, writes: map[string]string{}, saveDeltas: map[string]int{}, outcomes: map[string]stagedSaveOutcome{}}
	q := &stagedBatchQueue{events: events}
	e := &executor{smv2: store, queue: q, clock: clockwork.NewFakeClockAt(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)), log: logger.VoidLogger(), tracerProvider: tracing.NewNoopTracerProvider(), syncLifecycles: []execution.SyncLifecycleListener{&stagedBatchLifecycle{events: events}}}
	max := 3
	priority := int64(7)
	edge := inngest.Edge{Incoming: "function", Outgoing: "previous"}
	resp := &state.DriverResponse{Generator: ops, RequestVersion: 2}
	i := &runInstance{md: sv2.Metadata{ID: sv2.ID{RunID: ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAV"), FunctionID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Tenant: sv2.Tenant{EnvID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), AccountID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), AppID: uuid.MustParse("00000000-0000-0000-0000-000000000004")}}, Config: *sv2.InitConfig(&sv2.Config{RequestVersion: 2, Idempotency: "run-key"})}, edge: edge, resp: resp, item: queue.Item{Payload: queue.PayloadEdge{Edge: edge}, Attempt: 1, MaxAttempts: &max, PriorityFactor: &priority}}
	return e, i, store, q, events
}

// Literal bytes are independent answers. Malformed data is an internal owner
// input here, not a claim that a malformed SDK response passes ParseGenerator.
func TestStagedBatchOutputAndSaveAuthority(t *testing.T) {
	userErr := &state.UserError{Name: "Error", Message: "boom"}
	cases := []struct {
		name    string
		op      state.GeneratorOpcode
		outcome stagedSaveOutcome
		writes  map[string]string
		events  []string
		wantErr bool
	}{
		{name: "legacy-absent", op: state.GeneratorOpcode{Op: enums.OpcodeStep, ID: "s"}, writes: map[string]string{"s": ""}, events: []string{"handler:save:s", "finished:s", "handler:enqueue:s", "scheduled:edge:<nil>:0"}},
		{name: "legacy-null", op: state.GeneratorOpcode{Op: enums.OpcodeStep, ID: "s", Data: json.RawMessage(`null`)}, writes: map[string]string{"s": "null"}, events: []string{"handler:save:s", "finished:s", "handler:enqueue:s", "scheduled:edge:<nil>:0"}},
		{name: "legacy-passthrough-malformed", op: state.GeneratorOpcode{Op: enums.OpcodeStep, ID: "s", Data: json.RawMessage(`{`)}, writes: map[string]string{"s": "{"}, events: []string{"handler:save:s", "finished:s", "handler:enqueue:s", "scheduled:edge:<nil>:0"}},
		{name: "run-absent", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, writes: map[string]string{"s": `{"data":null}`}, events: []string{"handler:save:s", "finished:s", "handler:enqueue:s", "scheduled:edge:<nil>:0"}},
		{name: "run-whitespace-compacted", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s", Data: json.RawMessage(` { "x": 1 } `)}, writes: map[string]string{"s": `{"data":{"x":1}}`}, events: []string{"handler:save:s", "finished:s", "handler:enqueue:s", "scheduled:edge:<nil>:0"}},
		{name: "run-malformed-blocks-save", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s", Data: json.RawMessage(`{`)}, writes: map[string]string{}, wantErr: true},
		{name: "successful-save-pending", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, outcome: stagedSaveOutcome{pending: true}, writes: map[string]string{"s": `{"data":null}`}, events: []string{"handler:save:s", "finished:s", "scheduled:edge:<nil>:0"}},
		{name: "successful-duplicate-continues", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, outcome: stagedSaveOutcome{err: state.ErrDuplicateResponse}, writes: map[string]string{"s": `{"data":null}`}, events: []string{"handler:save:s", "finished:s", "handler:enqueue:s", "scheduled:edge:<nil>:0"}},
		{name: "successful-idempotent-retains-pending", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, outcome: stagedSaveOutcome{pending: true, err: state.ErrIdempotentResponse}, writes: map[string]string{"s": `{"data":null}`}, events: []string{"handler:save:s", "finished:s", "scheduled:edge:<nil>:0"}},
		{name: "successful-wrapped-duplicate-retains-pending", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, outcome: stagedSaveOutcome{pending: true, err: fmt.Errorf("save transport: %w", state.ErrDuplicateResponse)}, writes: map[string]string{"s": `{"data":null}`}, events: []string{"handler:save:s", "finished:s", "scheduled:edge:<nil>:0"}},
		{name: "save-failure-blocks-lifecycle", op: state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, outcome: stagedSaveOutcome{err: context.Canceled}, writes: map[string]string{"s": `{"data":null}`}, events: []string{"handler:save:s"}, wantErr: true},
		{name: "permanent-failure", op: state.GeneratorOpcode{Op: enums.OpcodeStepFailed, ID: "s", Error: userErr}, writes: map[string]string{"s": `{"error":{"name":"Error","message":"boom"}}`}, events: []string{"handler:save:s", "failed:s:Failed:1", "handler:enqueue:s", "scheduled:edge-error:<nil>:0"}},
		{name: "permanent-duplicate-is-error", op: state.GeneratorOpcode{Op: enums.OpcodeStepFailed, ID: "s", Error: userErr}, outcome: stagedSaveOutcome{err: state.ErrDuplicateResponse}, writes: map[string]string{"s": `{"error":{"name":"Error","message":"boom"}}`}, events: []string{"handler:save:s"}, wantErr: true},
		{name: "permanent-wrapped-duplicate-retains-identity", op: state.GeneratorOpcode{Op: enums.OpcodeStepFailed, ID: "s", Error: userErr}, outcome: stagedSaveOutcome{pending: true, err: fmt.Errorf("save transport: %w", state.ErrDuplicateResponse)}, writes: map[string]string{"s": `{"error":{"name":"Error","message":"boom"}}`}, events: []string{"handler:save:s"}, wantErr: true},
		{name: "permanent-idempotent-continues", op: state.GeneratorOpcode{Op: enums.OpcodeStepFailed, ID: "s", Error: userErr}, outcome: stagedSaveOutcome{err: state.ErrIdempotentResponse}, writes: map[string]string{"s": `{"error":{"name":"Error","message":"boom"}}`}, events: []string{"handler:save:s", "failed:s:Failed:1", "handler:enqueue:s", "scheduled:edge-error:<nil>:0"}},
		{name: "permanent-pending", op: state.GeneratorOpcode{Op: enums.OpcodeStepFailed, ID: "s", Error: userErr}, outcome: stagedSaveOutcome{pending: true}, writes: map[string]string{"s": `{"error":{"name":"Error","message":"boom"}}`}, events: []string{"handler:save:s", "failed:s:Failed:1", "scheduled:edge-error:<nil>:0"}},
		{name: "retryable-error-no-save", op: state.GeneratorOpcode{Op: enums.OpcodeStepError, ID: "s", Name: "retry", Error: userErr}, writes: map[string]string{}, events: []string{"failed:s:Errored:1", "scheduled::retry:2"}, wantErr: true},
		{name: "user-no-retry-converts-to-failed", op: state.GeneratorOpcode{Op: enums.OpcodeStepError, ID: "s", Error: &state.UserError{Name: "Error", Message: "boom", NoRetry: true}}, writes: map[string]string{"s": `{"error":{"name":"Error","message":"boom","noRetry":true}}`}, events: []string{"handler:save:s", "failed:s:Failed:1", "handler:enqueue:s", "scheduled:edge-error:<nil>:0"}},
		{name: "none-has-no-handler-effects", op: state.GeneratorOpcode{Op: enums.OpcodeNone, ID: "s"}, writes: map[string]string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, i, s, q, events := newStagedBatch(t, &tc.op)
			s.outcomes["s"] = tc.outcome
			err := e.HandleGeneratorResponse(context.Background(), i, i.resp)
			if tc.wantErr {
				require.Error(t, err)
				if tc.outcome.err != nil {
					require.Same(t, tc.outcome.err, err, "rejected saves retain the original error, including wrapping")
				}
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.writes, s.writes)
			if tc.name == "retryable-error-no-save" {
				require.ErrorIs(t, err, ErrHandledStepError)
			}
			require.Equal(t, tc.events, events.snapshot())
			if len(q.calls) > 0 {
				call := q.calls[0]
				require.Equal(t, time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC), call.at)
				require.Equal(t, inngest.Edge{Incoming: "function", Outgoing: "s"}, call.item.Payload.(queue.PayloadEdge).Edge)
				job := "00000000-0000-0000-0000-000000000001:run-key-s"
				if call.item.Kind == queue.KindEdgeError {
					job += "-failure"
				}
				require.Equal(t, job, *call.item.JobID)
				require.Equal(t, queue.EnqueueOpts{}, call.opts)
				require.Equal(t, int64(7), *call.item.PriorityFactor)
				require.Equal(t, 3, *call.item.MaxAttempts)
				require.Zero(t, call.item.Attempt)
				requireStagedQueueBindings(t, call)
			}
		})
	}
}

func TestStagedBatchStateOverflowPrecedesRetryPreferences(t *testing.T) {
	e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"})
	overflow := fmt.Errorf("save transport: %w", state.ErrStateOverflowed)
	s.outcomes["s"] = stagedSaveOutcome{err: overflow}
	i.resp.NoRetry = true
	retryAt := time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC)
	i.resp.RetryAt = &retryAt
	require.Same(t, overflow, e.HandleGeneratorResponse(context.Background(), i, i.resp))
	require.Equal(t, []string{"handler:save:s"}, events.snapshot())
	require.Empty(t, q.calls)
}

func TestStagedBatchGroupErrorNoRetryPrecedesRetryAt(t *testing.T) {
	e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"})
	s.outcomes["s"] = stagedSaveOutcome{err: fmt.Errorf("save unavailable")}
	i.resp.NoRetry = true
	retryAt := time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC)
	i.resp.RetryAt = &retryAt

	err := e.HandleGeneratorResponse(context.Background(), i, i.resp)
	require.False(t, queue.ShouldRetry(err, 0, 3), "response NoRetry wins over RetryAt for an ordinary group error")
	require.Nil(t, queue.AsRetryAtError(err))
	// NeverRetryError forwards the cause's message but does not implement Unwrap.
	require.EqualError(t, err, "save unavailable")
	require.Equal(t, []string{"handler:save:s"}, events.snapshot())
	require.Empty(t, q.calls)
}

func TestStagedBatchMetadataDeltaReachesSave(t *testing.T) {
	for _, op := range []enums.Opcode{enums.OpcodeStepRun, enums.OpcodeStepFailed} {
		t.Run(op.String(), func(t *testing.T) {
			g := &state.GeneratorOpcode{Op: op, ID: "s", Error: &state.UserError{Name: "E", Message: "x"}, Metadata: []metadata.ScopedUpdate{
				stepMetadataUpdate("userland.custom", metadata.Values{"k": json.RawMessage(`"v"`)}),
			}}
			e, i, s, _, _ := newStagedBatch(t, g)
			i.md.Metrics.MetadataSize = 11
			i.md.Metrics.MetadataSizeLoaded = 7
			require.NoError(t, e.HandleGeneratorResponse(context.Background(), i, i.resp))
			// Four previously unpersisted bytes plus the emitted metadata's
			// one-byte key and three-byte JSON value must reach the store.
			require.Equal(t, map[string]int{"s": 8}, s.saveDeltas)
		})
	}
}

func TestStagedBatchDiscoveryParallelModeAuthority(t *testing.T) {
	for _, tc := range []struct {
		name                string
		op                  enums.Opcode
		opRace, contextRace bool
		enqueues            int
	}{
		{"success-uses-opcode-race", enums.OpcodeStepRun, true, false, 1},
		{"success-ignores-context-race", enums.OpcodeStepRun, false, true, 0},
		{"failure-ignores-opcode-race", enums.OpcodeStepFailed, true, false, 0},
		{"failure-uses-context-race", enums.OpcodeStepFailed, false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &state.GeneratorOpcode{Op: tc.op, ID: "s", Error: &state.UserError{Name: "E", Message: "x"}}
			if tc.opRace {
				g.Opts = map[string]any{"parallelMode": "race"}
			}
			e, i, s, q, _ := newStagedBatch(t, g)
			if tc.contextRace {
				i.item.ParallelMode = enums.ParallelModeRace
			}
			s.outcomes["s"] = stagedSaveOutcome{pending: true}
			require.NoError(t, e.HandleGeneratorResponse(context.Background(), i, i.resp))
			require.Len(t, q.calls, tc.enqueues)
		})
	}
}

func TestStagedBatchMetadataAndPendingBarriers(t *testing.T) {
	cases := []struct {
		name                                 string
		stored, response                     int
		force, ai, failMetadata, failPending bool
		updates                              []sv2.MutableConfig
		pending                              bool
	}{
		{name: "stored-zero-disagrees", stored: 0, response: 2, updates: []sv2.MutableConfig{{RequestVersion: 2, ForceStepPlan: true}}},
		{name: "stored-one-disagrees", stored: 1, response: 2, updates: []sv2.MutableConfig{{RequestVersion: 2, ForceStepPlan: true}}},
		{name: "unknown-negotiates-zero", stored: -1, response: 0, updates: []sv2.MutableConfig{{RequestVersion: 0, ForceStepPlan: true}}},
		{name: "unknown-negotiates-one", stored: -1, response: 1, updates: []sv2.MutableConfig{{RequestVersion: 1, ForceStepPlan: true}}},
		{name: "unknown-negotiates-two", stored: -1, response: 2, updates: []sv2.MutableConfig{{RequestVersion: 2, ForceStepPlan: true}}, pending: true},
		{name: "future-stored-disagrees", stored: 7, response: 0, updates: []sv2.MutableConfig{{RequestVersion: 0, ForceStepPlan: true}}, pending: true},
		{name: "force-already-set", stored: 2, response: 1, force: true, pending: true},
		{name: "ai-proposal-retains-force", stored: 0, response: 2, force: true, ai: true, updates: []sv2.MutableConfig{{RequestVersion: 2, ForceStepPlan: true, HasAI: true}}},
		{name: "metadata-failure-before-pending", stored: -1, response: 2, failMetadata: true, updates: []sv2.MutableConfig{{RequestVersion: 2, ForceStepPlan: true}}},
		{name: "pending-failure-before-handlers", stored: 2, response: 2, force: true, failPending: true, pending: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "a"}
			b := &state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "b"}
			if tc.ai {
				a.Opts = map[string]any{"type": "step.ai.wrap"}
			}
			e, i, s, q, events := newStagedBatch(t, a, b)
			i.md.Config.RequestVersion = tc.stored
			i.md.Config.ForceStepPlan = tc.force
			i.resp.RequestVersion = tc.response
			if tc.failMetadata {
				s.metadataErr = context.Canceled
			}
			if tc.failPending {
				s.savePendingErr = context.Canceled
			}
			err := e.HandleGeneratorResponse(context.Background(), i, i.resp)
			if tc.failMetadata || tc.failPending {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, q.calls)
				require.Empty(t, s.writes)
			} else {
				require.NoError(t, err)
				require.Len(t, q.calls, 2)
			}
			pending, updates, _, _ := s.snapshot()
			require.Equal(t, tc.updates, updates)
			if tc.pending {
				require.Equal(t, [][]string{{"a", "b"}}, pending)
			} else {
				require.Empty(t, pending)
			}
			// Metadata persistence does not replace the snapshot driving this response.
			require.Equal(t, tc.stored, i.md.Config.RequestVersion)
			require.Equal(t, tc.force, i.md.Config.ForceStepPlan)
			if tc.failMetadata {
				require.Equal(t, []string{"metadata_update"}, events.snapshot())
			}
			if tc.failPending {
				require.Equal(t, []string{"save_pending_failed"}, events.snapshot())
			}
			if tc.pending && !tc.failPending {
				requirePendingBeforeHandlerEffects(t, events.snapshot())
			}
			for _, call := range q.calls {
				if tc.pending {
					require.NotNil(t, call.item.ParallelCoalesceKey)
				} else {
					require.Nil(t, call.item.ParallelCoalesceKey)
				}
			}
		})
	}
	// The step-limit gate is after metadata persistence, before pending/handlers.
	t.Run("metadata-before-step-limit", func(t *testing.T) {
		op := &state.GeneratorOpcode{Op: enums.OpcodeNone, ID: "n"}
		ops := make([]*state.GeneratorOpcode, consts.DefaultMaxStepLimit+1)
		for j := range ops {
			ops[j] = op
		}
		e, i, s, q, events := newStagedBatch(t, ops...)
		i.md.Config.RequestVersion = -1
		err := e.HandleGeneratorResponse(context.Background(), i, i.resp)
		require.ErrorIs(t, err, state.ErrFunctionOverflowed)
		require.Equal(t, []string{"metadata_update"}, events.snapshot())
		require.Empty(t, s.writes)
		require.Empty(t, q.calls)
	})
}

func TestStagedBatchRecipesAndObservationClocks(t *testing.T) {
	display := "display"
	cases := []struct {
		name        string
		op          state.GeneratorOpcode
		kind, job   string
		edge        inngest.Edge
		offset      time.Duration
		passthrough bool
		events      []string
		wantErr     bool
	}{
		{name: "planned-display-name", op: state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "p", Name: "sdk-name", DisplayName: &display, Opts: map[string]any{"parallelMode": "race"}}, kind: queue.KindEdge, job: "00000000-0000-0000-0000-000000000001:run-key-p-plan", edge: inngest.Edge{Incoming: "function", Outgoing: "previous", IncomingGeneratorStep: "p", IncomingGeneratorStepName: "display"}, offset: -time.Millisecond, events: []string{"metadata_update", "handler:enqueue:previous", "scheduled:edge:sdk-name:0"}},
		{name: "sleep-option-overrides-name", op: state.GeneratorOpcode{Op: enums.OpcodeSleep, ID: "z", Name: "99s", Opts: map[string]any{"duration": "2s"}}, kind: queue.KindSleep, edge: inngest.Edge{Incoming: "function", Outgoing: "z"}, offset: 2 * time.Second, passthrough: true, events: []string{"metadata_update", "handler:enqueue:z", "sleep:z"}},
		{name: "sleep-legacy-name", op: state.GeneratorOpcode{Op: enums.OpcodeSleep, ID: "z", Name: "3s"}, kind: queue.KindSleep, edge: inngest.Edge{Incoming: "function", Outgoing: "z"}, offset: 3 * time.Second, passthrough: true, events: []string{"metadata_update", "handler:enqueue:z", "sleep:z"}},
		{name: "sleep-invalid-stops-recipe", op: state.GeneratorOpcode{Op: enums.OpcodeSleep, ID: "z", Name: "invalid-duration"}, events: []string{"metadata_update"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, i, s, q, events := newStagedBatch(t, &tc.op)
			i.md.Config.RequestVersion = -1
			clock := e.clock.(*clockwork.FakeClock)
			// Persistence advances the clock. A future planner must not pre-sample the
			// handler's scheduling time before the metadata write has completed.
			s.afterMetadata = func() { clock.Advance(5 * time.Second) }
			err := e.HandleGeneratorResponse(context.Background(), i, i.resp)
			if tc.wantErr {
				require.Error(t, err)
				require.Empty(t, q.calls)
			} else {
				require.NoError(t, err)
				require.Len(t, q.calls, 1)
				call := q.calls[0]
				require.Equal(t, tc.kind, call.item.Kind)
				require.Equal(t, tc.edge, call.item.Payload.(queue.PayloadEdge).Edge)
				require.Equal(t, time.Date(2026, 10, 26, 12, 0, 5, 0, time.UTC).Add(tc.offset), call.at)
				require.Equal(t, queue.EnqueueOpts{PassthroughJobId: tc.passthrough}, call.opts)
				if tc.job != "" {
					require.Equal(t, tc.job, *call.item.JobID)
				} else {
					require.Equal(t, "qfb8aemkm1th", *call.item.JobID)
				}
				if tc.name == "planned-display-name" {
					require.Equal(t, enums.ParallelModeRace, call.item.ParallelMode)
				}
				requireStagedQueueBindings(t, call)
				require.Zero(t, call.item.Attempt)
				require.Equal(t, 3, *call.item.MaxAttempts)
			}
			require.Equal(t, tc.events, events.snapshot())
		})
	}
}

// Siblings rendezvous at SaveStep before either may return. Quiescence after
// releasing the failed save lets its handler finish while b remains blocked;
// response completion must still wait for b. Sibling entry order is not a contract.
func TestStagedBatchSiblingJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "a"}
		b := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "b"}
		e, i, s, _, events := newStagedBatch(t, a, b)
		entered := make(chan string, 2)
		releaseA, releaseB := make(chan struct{}), make(chan struct{})
		unblockA := sync.OnceFunc(func() { close(releaseA) })
		unblockB := sync.OnceFunc(func() { close(releaseB) })
		done := make(chan error, 1)
		s.beforeSave = func(id string) {
			entered <- id
			if id == "a" {
				<-releaseA
			} else {
				<-releaseB
			}
		}
		s.outcomes["a"] = stagedSaveOutcome{err: context.Canceled}
		// Run even on Fatal: release both saves and drain runnable work, including
		// orphaned siblings under a broken join, before leaving the bubble.
		defer func() {
			unblockA()
			unblockB()
			synctest.Wait()
		}()
		go func() { done <- e.HandleGeneratorResponse(context.Background(), i, i.resp) }()

		synctest.Wait()
		var entries []string
		for len(entered) > 0 {
			entries = append(entries, <-entered)
		}
		require.ElementsMatch(t, []string{"a", "b"}, entries, "siblings did not reach SaveStep concurrently")
		require.NotContains(t, events.snapshot(), "handler:save:a")
		require.NotContains(t, events.snapshot(), "handler:save:b")

		unblockA()
		// Wait is a scheduler quiescence barrier, not an error-about-to-return
		// notification: a has no remaining blocking port after its failed save.
		// Only b's save and the owner's join can remain durably blocked here.
		synctest.Wait()
		require.Contains(t, events.snapshot(), "handler:save:a")
		require.NotContains(t, events.snapshot(), "handler:save:b")
		select {
		case err := <-done:
			t.Fatalf("returned before blocked sibling b finished after a failed: %v", err)
		default:
		}

		unblockB()
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		default:
			t.Fatal("group did not join after both siblings were released")
		}
		require.Contains(t, events.snapshot(), "finished:b")
	})
}

func TestStagedBatchMixedAcceptedSaveOutcomes(t *testing.T) {
	// All seven candidate tags in one original ordered batch. Actual SaveStep
	// results deliberately disagree between siblings; no pre-read can replace them.
	e, i, s, q, events := newStagedBatch(t,
		&state.GeneratorOpcode{Op: enums.OpcodeNone, ID: "none"},
		&state.GeneratorOpcode{Op: enums.OpcodeStep, ID: "legacy", Data: json.RawMessage(`{"old":true}`)},
		&state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "run", Data: json.RawMessage(`null`)},
		&state.GeneratorOpcode{Op: enums.OpcodeStepError, ID: "error", Error: &state.UserError{Name: "E", Message: "x", NoRetry: true}},
		&state.GeneratorOpcode{Op: enums.OpcodeStepFailed, ID: "failed", Error: &state.UserError{Name: "E", Message: "x"}},
		&state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "planned", Name: "plan"},
		&state.GeneratorOpcode{Op: enums.OpcodeSleep, ID: "sleep", Name: "1s"})
	s.outcomes["legacy"] = stagedSaveOutcome{pending: true}
	s.outcomes["error"] = stagedSaveOutcome{pending: true}
	// No mutable force-plan reset in this witness: it is about save authority,
	// not competing updates to metadata. Version 1 still enables parallel handlers.
	i.md.Config.RequestVersion = 1
	i.md.Config.ForceStepPlan = true
	require.NoError(t, e.HandleGeneratorResponse(context.Background(), i, i.resp))
	require.Equal(t, map[string]string{"legacy": `{"old":true}`, "run": `{"data":null}`, "error": `{"error":{"name":"E","message":"x","noRetry":true}}`, "failed": `{"error":{"name":"E","message":"x"}}`}, s.writes)
	var outgoing []string
	for _, call := range q.calls {
		outgoing = append(outgoing, call.item.Payload.(queue.PayloadEdge).Edge.Outgoing)
	}
	require.ElementsMatch(t, []string{"run", "failed", "previous", "sleep"}, outgoing)
	// Independently assert each local barrier without imposing a sibling order.
	observed := events.snapshot()
	for _, id := range []string{"legacy", "run", "error", "failed"} {
		save := -1
		finish := -1
		for index, event := range observed {
			if event == "handler:save:"+id {
				save = index
			}
			if event == "finished:"+id || event == "failed:"+id+":Failed:1" {
				finish = index
			}
		}
		require.Greater(t, finish, save)
		require.GreaterOrEqual(t, save, 0)
	}
	require.NotContains(t, observed, "handler:enqueue:legacy")
	require.NotContains(t, observed, "handler:enqueue:error")
}

func TestStagedBatchFinalAttemptAndSDKNoRetry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		attempt int
		noRetry bool
	}{
		{"attempt-exhausted", 2, false}, {"sdk-no-retry", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepError, ID: "s", Error: &state.UserError{Name: "E", Message: "x"}})
			i.item.Attempt = tc.attempt
			i.resp.NoRetry = tc.noRetry
			require.NoError(t, e.HandleGeneratorResponse(context.Background(), i, i.resp))
			require.Equal(t, map[string]string{"s": `{"error":{"name":"E","message":"x"}}`}, s.writes)
			require.Len(t, q.calls, 1)
			require.Equal(t, queue.KindEdgeError, q.calls[0].item.Kind)
			require.Contains(t, events.snapshot(), fmt.Sprintf("failed:s:Failed:%d", tc.attempt))
		})
	}
}

type stagedReleaseContext struct {
	*runInstance
	events *opcodePlanEvents
}

func (r *stagedReleaseContext) ReleaseCapacityLease() error { r.events.record("release"); return nil }

func TestStagedBatchSaveBeforeCapacityRelease(t *testing.T) {
	// The production queue lease's callback is private to package queue. The
	// existing RunContext release port lets the checkpoint owner observe ordering
	// without exporting that callback or changing the response owner.
	for _, op := range []enums.Opcode{enums.OpcodeStepRun, enums.OpcodeStepFailed} {
		for _, saveErr := range []error{nil, context.Canceled} {
			t.Run(fmt.Sprintf("%s/save-error-%t", op, saveErr != nil), func(t *testing.T) {
				g := state.GeneratorOpcode{Op: op, ID: "s", Error: &state.UserError{Name: "E", Message: "x"}}
				e, i, s, q, events := newStagedBatch(t, &g)
				s.outcomes["s"] = stagedSaveOutcome{err: saveErr}
				err := e.HandleGenerator(context.Background(), &stagedReleaseContext{runInstance: i, events: events}, g)
				if saveErr != nil {
					require.ErrorIs(t, err, saveErr)
					require.Equal(t, []string{"handler:save:s"}, events.snapshot())
					require.Empty(t, q.calls)
				} else {
					require.NoError(t, err)
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

type stagedTickClock struct {
	clockwork.Clock
	mu sync.Mutex
	at time.Time
}

func (c *stagedTickClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.at
	c.at = c.at.Add(time.Second)
	return at
}

type stagedSleepTimes struct {
	execution.NoopSyncLifecycleListener
	until, observed time.Time
}

func (l *stagedSleepTimes) OnSleep(_ context.Context, _ sv2.Metadata, _ queue.Item, _ state.GeneratorOpcode, until, observed time.Time) {
	l.until = until
	l.observed = observed
}

func TestStagedBatchSleepDistinctClockBindings(t *testing.T) {
	e, i, _, q, _ := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeSleep, ID: "sleep", Name: "2s"})
	base := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	e.clock = &stagedTickClock{Clock: clockwork.NewFakeClockAt(base), at: base}
	times := &stagedSleepTimes{}
	e.syncLifecycles = []execution.SyncLifecycleListener{times}
	tracer := newRecordingTracerProvider()
	e.tracerProvider = tracer
	require.NoError(t, e.HandleGeneratorResponse(context.Background(), i, i.resp))
	require.Len(t, q.calls, 1)
	// Handling is observed at 12:00:00. The later sleep scheduling read is
	// 12:00:01 + 2s, and the callback observes 12:00:02 independently.
	require.Equal(t, time.Date(2026, 10, 26, 12, 0, 3, 0, time.UTC), q.calls[0].at)
	require.Equal(t, time.Date(2026, 10, 26, 12, 0, 3, 0, time.UTC), times.until)
	require.Equal(t, time.Date(2026, 10, 26, 12, 0, 2, 0, time.UTC), times.observed)
	require.Len(t, tracer.createCalls, 2)
	queued, ok := meta.GetAttr(tracer.createCalls[0].opts.Attributes, meta.Attrs.QueuedAt)
	require.True(t, ok)
	require.Equal(t, base, *queued)
}

func TestStagedBatchStateSizeObservationIsNotReservation(t *testing.T) {
	a := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "a"}
	b := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "b"}
	e, i, s, _, _ := newStagedBatch(t, a, b)
	// Each wrapped null is 13 bytes. Both checks observe the same loaded size;
	// this does not reserve 26 bytes or authorize replacing storage accounting.
	e.stateSizeLimit = func(sv2.ID) int { return 13 }
	require.NoError(t, e.HandleGeneratorResponse(context.Background(), i, i.resp))
	require.Equal(t, map[string]string{"a": `{"data":null}`, "b": `{"data":null}`}, s.writes)
	require.Zero(t, i.md.Metrics.StateSize)
}

func TestStagedBatchQueueDuplicateSuppressesPostEnqueueCallbacks(t *testing.T) {
	for _, op := range []enums.Opcode{enums.OpcodeStepRun, enums.OpcodeStepFailed, enums.OpcodeStepPlanned, enums.OpcodeSleep} {
		t.Run(op.String(), func(t *testing.T) {
			g := state.GeneratorOpcode{Op: op, ID: "s", Name: "1s", Error: &state.UserError{Name: "E", Message: "x"}}
			e, i, _, q, events := newStagedBatch(t, &g)
			q.err = queue.ErrQueueItemExists
			require.NoError(t, e.HandleGeneratorResponse(context.Background(), i, i.resp))
			require.Len(t, q.calls, 1)
			want := map[enums.Opcode][]string{
				enums.OpcodeStepRun:     {"handler:save:s", "finished:s", "handler:enqueue:s"},
				enums.OpcodeStepFailed:  {"handler:save:s", "failed:s:Failed:1", "handler:enqueue:s"},
				enums.OpcodeStepPlanned: {"handler:enqueue:previous"},
				enums.OpcodeSleep:       {"handler:enqueue:s"},
			}
			require.Equal(t, want[op], events.snapshot())
		})
	}
}

func TestStagedBatchPlannedSleepEnqueueFailurePreferences(t *testing.T) {
	for _, op := range []enums.Opcode{enums.OpcodeStepPlanned, enums.OpcodeSleep} {
		for _, noRetry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/no-retry-%t", op, noRetry), func(t *testing.T) {
				e, i, _, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: op, ID: "s", Name: "1s"})
				retryAt := time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC)
				i.resp.NoRetry = noRetry
				i.resp.RetryAt = &retryAt
				q.err = context.Canceled
				err := e.HandleGeneratorResponse(context.Background(), i, i.resp)
				require.Error(t, err)
				require.False(t, queue.IsAlwaysRetryable(err))
				if noRetry {
					require.False(t, queue.ShouldRetry(err, 0, 3))
					require.Nil(t, queue.AsRetryAtError(err))
				} else {
					at := queue.AsRetryAtError(err)
					require.NotNil(t, at)
					require.Equal(t, retryAt, *at.NextRetryAt())
				}
				// These two handlers retain post-enqueue callbacks even on a real enqueue
				// failure, unlike the early return after ErrQueueItemExists.
				if op == enums.OpcodeSleep {
					require.Equal(t, []string{"handler:enqueue:s", "sleep:s"}, events.snapshot())
				} else {
					require.Equal(t, []string{"handler:enqueue:previous", "scheduled:edge:1s:0"}, events.snapshot())
				}
			})
		}
	}
}

func requireStagedQueueBindings(t *testing.T, call stagedEnqueue) {
	t.Helper()
	require.NotEqual(t, uuid.Nil, uuid.MustParse(call.item.GroupID))
	require.Equal(t, call.item.GroupID, call.contextGroup)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000002"), call.item.WorkspaceID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000001"), call.item.Identifier.WorkflowID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000003"), call.item.Identifier.AccountID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000004"), call.item.Identifier.AppID)
	require.Equal(t, ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAV"), call.item.Identifier.RunID)
}
