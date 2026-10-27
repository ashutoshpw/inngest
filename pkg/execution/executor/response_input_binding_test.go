package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Prospective Go-only input contract, NOT a production/native adapter. Only
// source values live here; preparation, metadata proposals, SaveStep answers,
// queue recipes, clocks, fresh UUIDs and generated carriers are observations.
// The resolver is the actual setting handle, not an eagerly resolved answer.
// Its activation and nil/zero semantics remain owned by validateStateSize.
type stepRunInputFacts struct {
	loaded          sv2.Metadata
	original        state.GeneratorOpcode
	responseVersion int
	noRetry         bool
	retryAt         *time.Time
	// JSON projection only: excludes JobID, EnqueuedAt, At and runtime handles.
	item           queue.Item
	edge           queue.PayloadEdge
	contextGroup   string
	stateSizeLimit func(sv2.ID) int
}

func bindingSnapshot[T any](t *testing.T, source T) T {
	t.Helper()
	b, err := json.Marshal(source)
	require.NoError(t, err)
	var snapshot T
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&snapshot))
	return snapshot
}

func bindStepRunInputs(t *testing.T, ctx context.Context, e *executor, run execution.RunContext, resp *state.DriverResponse) stepRunInputFacts {
	t.Helper()
	require.Len(t, resp.Generator, 1)
	require.NotNil(t, resp.Generator[0])
	gen := *resp.Generator[0]
	require.Equal(t, enums.OpcodeStepRun, gen.Op)
	require.False(t, gen.HasAI())
	require.Nil(t, gen.Error)
	// Preserve raw data (including malformed JSON) without Output(), compaction,
	// or a JSON round-trip of that data. Clone the rest of the descriptor too.
	data := slices.Clone(gen.Data)
	var opts json.RawMessage
	if raw, ok := gen.Opts.(json.RawMessage); ok {
		opts = slices.Clone(raw)
	} else if gen.Opts != nil {
		var err error
		opts, err = json.Marshal(gen.Opts)
		require.NoError(t, err)
	}
	gen.Data, gen.Opts = nil, nil
	gen = bindingSnapshot(t, gen)
	gen.Data = data
	if opts != nil {
		gen.Opts = opts
	}
	item := run.LifecycleItem()
	edge, ok := item.Payload.(queue.PayloadEdge)
	require.True(t, ok)
	item = bindingSnapshot(t, item)
	item.Payload = bindingSnapshot(t, edge)
	return stepRunInputFacts{
		loaded: bindingSnapshot(t, *run.Metadata()), original: gen,
		responseVersion: resp.RequestVersion, noRetry: resp.NoRetry,
		retryAt: bindingSnapshot(t, resp.RetryAt),
		item:    item, edge: bindingSnapshot(t, edge),
		contextGroup: state.GroupIDFromContext(ctx), stateSizeLimit: e.stateSizeLimit,
	}
}

// Existing literals remain the only policy oracle. This is an extra observation
// at their actual owner call, not a second invocation/replay of the corpus.
func runBoundStepRunContract(t *testing.T, ctx context.Context, e *executor, i *runInstance, resp *state.DriverResponse) error {
	t.Helper()
	if len(resp.Generator) != 1 || resp.Generator[0] == nil || resp.Generator[0].Op != enums.OpcodeStepRun || resp.Generator[0].HasAI() || resp.Generator[0].Error != nil {
		return e.HandleGeneratorResponse(ctx, i, resp)
	}
	facts := bindStepRunInputs(t, ctx, e, i, resp)
	s := e.smv2.(*stagedBatchStore)
	err := e.HandleGeneratorResponse(ctx, i, resp)
	require.Equal(t, facts.loaded.Config.RequestVersion, i.md.Config.RequestVersion, "proposal cannot refresh the current response's loaded version")
	require.Equal(t, facts.loaded.Config.ForceStepPlan, i.md.Config.ForceStepPlan)
	pending, _, _, _ := s.snapshot()
	require.Empty(t, pending, "single StepRun never replaces the pending set")
	for _, save := range s.saves {
		require.Equal(t, facts.loaded.ID, save.runID)
		require.Equal(t, facts.original.ID, save.stepID)
	}
	return err
}

func TestStepRunInputBindingSourceChanges(t *testing.T) {
	// These are changes to actual entry objects, not a fixture-derived binding.
	// The portable preparation fixture remains the literal byte oracle.
	for _, prepared := range nativePreparationCases(t) {
		t.Run(prepared.Name, func(t *testing.T) {
			g := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "changed", Data: json.RawMessage(prepared.Data)}
			if prepared.Data == "" {
				g.Data = nil
			}
			e, i, s, q, _ := newStagedBatch(t, g)
			clock := &continuationClock{Clock: e.clock, base: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)}
			e.clock = clock
			exporter := tracetest.NewInMemoryExporter()
			e.tracerProvider = tracing.NewOtelTracerProvider(exporter, 0)
			i.md.ID.FunctionID = uuid.MustParse("00000000-0000-0000-0000-000000000009")
			i.md.Config.Idempotency = "changed-key"
			i.md.Config.RequestVersion = -1
			i.md.Metrics.StateSize = 34
			i.md.Metrics.MetadataSize = 11
			i.md.Metrics.MetadataSizeLoaded = 7
			i.item.PriorityFactor = new(int64(19))
			i.item.MaxAttempts = new(8)
			i.item.Payload = queue.PayloadEdge{Edge: i.edge}
			edge := i.item.Payload.(queue.PayloadEdge)
			edge.Edge.Incoming = "changed-incoming"
			i.item.Payload = edge
			// Deliberately different from i.DriverResponse(): the explicit response is
			// the HandleGeneratorResponse source for staging, version and raw opcode.
			resp := &state.DriverResponse{RequestVersion: 3, Generator: []*state.GeneratorOpcode{g}}
			i.resp = &state.DriverResponse{RequestVersion: 99}
			limitCalls := 0
			e.stateSizeLimit = func(id sv2.ID) int {
				limitCalls++
				require.Equal(t, i.md.ID, id)
				return 1024
			}
			ctx := state.WithGroupID(context.Background(), "input-group")
			facts := bindStepRunInputs(t, ctx, e, i, resp)
			require.Zero(t, limitCalls, "binding must not activate the limit resolver")
			require.Zero(t, clock.count)
			require.Empty(t, exporter.GetSpans())
			// Change the effect answer only AFTER binding. It must stay authoritative;
			// no pending/save-completed/enqueue answer is an initial source fact.
			var saveErr error
			if prepared.Name == "missing" {
				saveErr = fmt.Errorf("existing output: %w", state.ErrDuplicateResponse)
				s.outcomes["changed"] = stagedSaveOutcome{pending: true, err: saveErr}
			}
			require.Equal(t, -1, facts.loaded.Config.RequestVersion)
			require.Equal(t, 3, facts.responseVersion)
			require.Equal(t, "changed", facts.original.ID)
			require.Equal(t, prepared.Data, string(facts.original.Data))
			require.Equal(t, 34, facts.loaded.Metrics.StateSize)
			require.Equal(t, "changed-incoming", facts.edge.Edge.Incoming)
			require.Equal(t, int64(19), *facts.item.PriorityFactor)
			require.Equal(t, 8, *facts.item.MaxAttempts)
			require.Equal(t, "input-group", facts.contextGroup)
			s.afterMetadata = func() {
				require.Equal(t, -1, facts.loaded.Config.RequestVersion)
				require.Equal(t, -1, i.md.Config.RequestVersion)
			}
			require.NoError(t, e.HandleGeneratorResponse(ctx, i, resp))
			require.Equal(t, 1, limitCalls)
			pending, proposals, _, _ := s.snapshot()
			require.Empty(t, pending)
			require.Equal(t, []sv2.MutableConfig{{RequestVersion: 3}}, proposals)
			require.Equal(t, -1, facts.loaded.Config.RequestVersion)
			require.Equal(t, -1, i.md.Config.RequestVersion)
			require.Len(t, s.saves, 1)
			save := s.saves[0]
			require.Equal(t, facts.loaded.ID, save.runID)
			require.Equal(t, "changed", save.stepID)
			require.Equal(t, prepared.Prepared, save.output)
			require.Equal(t, 4, save.metadataDelta)
			if saveErr != nil {
				require.Same(t, saveErr, save.result.err)
			} else {
				require.NoError(t, save.result.err)
			}
			// These are actual activation observations, kept outside the initial facts.
			// Do not infer that tracing is absent even on an ordinary non-AI StepRun.
			require.NotEmpty(t, exporter.GetSpans())
			t.Logf("activation observations: clock reads=%d spans=%d save=%+v queue=%+v", clock.count, len(exporter.GetSpans()), save, q.calls)
			if prepared.Name == "missing" {
				require.True(t, save.result.pending)
				require.Empty(t, q.calls, "the returned pending result suppresses enqueue")
			} else {
				require.False(t, save.result.pending)
				require.Len(t, q.calls, 1)
				actual := q.calls[0].item
				require.Equal(t, "00000000-0000-0000-0000-000000000009:changed-key-changed", *actual.JobID)
				require.Equal(t, "changed-incoming", actual.Payload.(queue.PayloadEdge).Edge.Incoming)
				require.Equal(t, int64(19), *actual.PriorityFactor)
				require.Equal(t, 8, *actual.MaxAttempts)
				require.Equal(t, facts.loaded.ID.FunctionID, actual.Identifier.WorkflowID)
				require.Equal(t, actual.GroupID, q.calls[0].contextGroup)
				_, err := uuid.Parse(actual.GroupID)
				require.NoError(t, err)
				require.NotEqual(t, "input-group", actual.GroupID)
				require.NotEmpty(t, actual.Metadata[meta.PropagationKey], "carrier is materialized by the actual Go trace effect")
			}
			// A later source edit cannot rewrite an already-bound snapshot, including
			// pointer/slice/map fields. This also keeps proposals distinct from inputs.
			i.md.Config.RequestVersion = 77
			*i.item.PriorityFactor = 88
			if len(g.Data) > 0 {
				g.Data[0] = '!'
			}
			require.Equal(t, -1, facts.loaded.Config.RequestVersion)
			require.Equal(t, int64(19), *facts.item.PriorityFactor)
			require.Equal(t, prepared.Data, string(facts.original.Data))
		})
	}
}

// The corpus supplies the same response through both paths. These cases protect
// the missing distinction at the real SaveStep-error owner, without replaying its
// policy matrix. Facts never determine either the store answer or expectations.
func TestStepRunInputBindingResponsePreferences(t *testing.T) {
	base := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name                            string
		explicitNoRetry, contextNoRetry bool
		explicitRetry, contextRetry     *time.Time
	}{
		{name: "explicit-never", explicitNoRetry: true, contextRetry: new(base.Add(2 * time.Hour))},
		{name: "explicit-time", explicitRetry: new(base.Add(time.Hour)), contextRetry: new(base.Add(2 * time.Hour))},
		{name: "explicit-time-over-context-never", explicitRetry: new(base.Add(time.Hour)), contextNoRetry: true},
		{name: "explicit-default", contextNoRetry: true, contextRetry: new(base.Add(2 * time.Hour))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s", Data: json.RawMessage(`null`)}
			e, i, s, q, _ := newStagedBatch(t, g)
			resp := &state.DriverResponse{RequestVersion: 2, Generator: []*state.GeneratorOpcode{g}, NoRetry: tc.explicitNoRetry, RetryAt: tc.explicitRetry}
			i.resp = &state.DriverResponse{NoRetry: tc.contextNoRetry, RetryAt: tc.contextRetry, UserError: &state.UserError{NoRetry: true}}
			facts := bindStepRunInputs(t, context.Background(), e, i, resp)
			require.Equal(t, tc.explicitNoRetry, facts.noRetry)
			if tc.explicitRetry == nil {
				require.Nil(t, facts.retryAt)
			} else {
				require.NotNil(t, facts.retryAt)
				require.Equal(t, base.Add(time.Hour), *facts.retryAt)
			}
			// Supply the actual failure after binding, retaining both port return values.
			saveErr := errors.New("save unavailable")
			s.outcomes["s"] = stagedSaveOutcome{pending: true, err: saveErr}
			err := e.HandleGeneratorResponse(context.Background(), i, resp)
			if tc.name == "explicit-never" {
				// NeverRetryError preserves the message, but does not unwrap.
				require.EqualError(t, err, "save unavailable")
			} else {
				require.ErrorIs(t, err, saveErr)
			}
			require.Len(t, s.saves, 1)
			require.Same(t, saveErr, s.saves[0].result.err)
			require.True(t, s.saves[0].result.pending)
			require.Equal(t, `{"data":null}`, s.saves[0].output)
			require.Empty(t, q.calls)
			switch tc.name {
			case "explicit-never":
				require.False(t, queue.ShouldRetry(err, 0, 3))
				require.Nil(t, queue.AsRetryAtError(err))
			case "explicit-time", "explicit-time-over-context-never":
				at := queue.AsRetryAtError(err)
				require.NotNil(t, at)
				require.Equal(t, base.Add(time.Hour), *at.NextRetryAt())
				require.True(t, queue.ShouldRetry(err, 0, 3))
			case "explicit-default":
				require.Same(t, saveErr, err)
				require.True(t, queue.ShouldRetry(err, 0, 3))
				require.Nil(t, queue.AsRetryAtError(err))
			}
			resp.NoRetry = !tc.explicitNoRetry
			if resp.RetryAt != nil {
				*resp.RetryAt = base.Add(3 * time.Hour)
				require.Equal(t, base.Add(time.Hour), *facts.retryAt, "snapshot must survive a later pointer edit")
			}
			require.Equal(t, tc.explicitNoRetry, facts.noRetry)
		})
	}
}
