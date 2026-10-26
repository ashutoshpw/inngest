package executor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

const opcodePlanSourceCommit = "3624c37d5285805d2a95efd0715e8913d5f81ae8"

type opcodePlanFixture struct {
	SchemaVersion     int                  `json:"schemaVersion"`
	Contract          string               `json:"contract"`
	Source            opcodePlanProvenance `json:"source"`
	Cases             []opcodePlanCase     `json:"cases"`
	CoalesceHashCases []coalesceHashCase   `json:"coalesceHashCases"`
}

type opcodePlanProvenance struct {
	Repository string   `json:"repository"`
	Commit     string   `json:"commit"`
	Files      []string `json:"files"`
}

type opcodeFixtureOpcode struct {
	Op enums.Opcode `json:"op"`
	ID string       `json:"id"`
}

type opcodePlanCase struct {
	Name                   string                   `json:"name"`
	RunID                  string                   `json:"runId"`
	StoredRequestVersion   int                      `json:"storedRequestVersion"`
	ResponseRequestVersion int                      `json:"responseRequestVersion"`
	ForceStepPlan          bool                     `json:"forceStepPlan"`
	HandlerCase            bool                     `json:"handlerCase"`
	FailSavePending        bool                     `json:"failSavePending"`
	Input                  []*state.GeneratorOpcode `json:"input"`
	Expected               opcodePlanExpected       `json:"expected"`
}

type opcodePlanExpected struct {
	PriorityIndices []int                      `json:"priorityIndices"`
	OtherIndices    []int                      `json:"otherIndices"`
	NonLazyIndices  []int                      `json:"nonLazyIndices"`
	Priority        []opcodeFixtureOpcode      `json:"priority"`
	Other           []opcodeFixtureOpcode      `json:"other"`
	NonLazyIDs      []string                   `json:"nonLazyIds"`
	HistoryGroup    bool                       `json:"historyGroup"`
	HasPlanOp       bool                       `json:"hasPlanOp"`
	ShouldCoalesce  bool                       `json:"shouldCoalesce"`
	CoalesceKey     string                     `json:"coalesceKey"`
	MetadataUpdates []opcodePlanMetadataUpdate `json:"metadataUpdates"`
	HandlerEnqueues int                        `json:"handlerEnqueues"`
	HandlerError    bool                       `json:"handlerError"`
}

type opcodePlanMetadataUpdate struct {
	RequestVersion int  `json:"requestVersion"`
	ForceStepPlan  bool `json:"forceStepPlan"`
}

type coalesceHashCase struct {
	Name                   string   `json:"name"`
	RunID                  string   `json:"runId"`
	StepIDs                []string `json:"stepIds"`
	ExpectedKey            string   `json:"expectedKey"`
	ExpectedDiscoveryJobID string   `json:"expectedDiscoveryJobId"`
	SameKeyAs              string   `json:"sameKeyAs"`
	DifferentKeyFrom       string   `json:"differentKeyFrom"`
}

func loadOpcodePlanFixture(t *testing.T) opcodePlanFixture {
	t.Helper()

	data, err := os.ReadFile("testdata/compatibility/opcode-plan-v1.json")
	require.NoError(t, err)

	var fixture opcodePlanFixture
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	require.Equal(t, "opcode-plan-v1", fixture.Contract)
	require.Equal(t, opcodePlanSourceCommit, fixture.Source.Commit)
	return fixture
}

func fixtureOpcodes(input []*state.GeneratorOpcode) []opcodeFixtureOpcode {
	result := make([]opcodeFixtureOpcode, 0, len(input))
	for _, op := range input {
		result = append(result, opcodeFixtureOpcode{Op: op.Op, ID: op.ID})
	}
	return result
}

func TestOpcodePlanCompatibilityFixture(t *testing.T) {
	fixture := loadOpcodePlanFixture(t)
	seen := make(map[string]struct{}, len(fixture.Cases))

	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			_, exists := seen[tc.Name]
			require.False(t, exists, "fixture case names must be unique")
			seen[tc.Name] = struct{}{}

			ops := tc.Input
			originalIndices := make(map[*state.GeneratorOpcode]int, len(ops))
			for index, op := range ops {
				require.NotNil(t, op, "this corpus covers non-null opcodes only")
				originalIndices[op] = index
			}
			groups := opGroups(ops)
			// Map the real owner's pointers, never IDs: duplicate IDs and even
			// identical op/id pairs must retain their original payload and position.
			groupIndices := make([][]int, 0, 2)
			for _, group := range groups.All() {
				indices := make([]int, 0, len(group.Opcodes))
				for _, op := range group.Opcodes {
					index, exists := originalIndices[op]
					require.True(t, exists, "group must retain the original opcode")
					indices = append(indices, index)
				}
				groupIndices = append(groupIndices, indices)
			}
			require.NotNil(t, tc.Expected.PriorityIndices)
			require.NotNil(t, tc.Expected.OtherIndices)
			require.Equal(t, [][]int{tc.Expected.PriorityIndices, tc.Expected.OtherIndices}, groupIndices)
			require.Equal(t, tc.Expected.Priority, fixtureOpcodes(groups.PriorityGroup.Opcodes))
			require.Equal(t, tc.Expected.Other, fixtureOpcodes(groups.OtherGroup.Opcodes))
			// The arrays pin classification and stable filtering order only. Each
			// group is handled concurrently, so sibling effect order is not a contract.
			require.Equal(t, tc.Expected.HistoryGroup, groups.PriorityGroup.ShouldStartHistoryGroup)
			require.Equal(t, tc.Expected.HistoryGroup, groups.OtherGroup.ShouldStartHistoryGroup)

			nonLazyIDs := groups.NonLazyIDs()
			require.Equal(t, tc.Expected.NonLazyIDs, nonLazyIDs)
			// NonLazyIDs exposes strings rather than pointers. Independently map
			// the literal index answer to originals without copying its lazy predicate.
			require.NotNil(t, tc.Expected.NonLazyIndices)
			indexedIDs := make([]string, 0, len(tc.Expected.NonLazyIndices))
			indexLabels := make([]string, 0, len(tc.Expected.NonLazyIndices))
			for _, index := range tc.Expected.NonLazyIndices {
				require.GreaterOrEqual(t, index, 0)
				require.Less(t, index, len(ops))
				indexedIDs = append(indexedIDs, ops[index].ID)
				indexLabels = append(indexLabels, strconv.Itoa(index))
			}
			require.Equal(t, tc.Expected.NonLazyIDs, indexedIDs)
			// A second projection through the real owner disambiguates non-lazy
			// positions even when original IDs repeat or are empty. Copies leave
			// the original opcode objects intact; no classification is copied here.
			positionOps := make([]*state.GeneratorOpcode, 0, len(ops))
			for index, op := range ops {
				copied := *op
				copied.ID = strconv.Itoa(index)
				positionOps = append(positionOps, &copied)
			}
			require.Equal(t, indexLabels, opGroups(positionOps).NonLazyIDs())
			require.Equal(t, len(tc.Expected.NonLazyIDs), nonLazyOpCount(ops))
			require.Equal(t, tc.Expected.HistoryGroup, nonLazyOpCount(ops) > 1)

			require.Equal(t, tc.Expected.HasPlanOp, hasPlanOp(ops))
		})
	}
	require.Len(t, seen, len(fixture.Cases))
}

func TestOpcodePlanCoalesceHashKnownAnswers(t *testing.T) {
	fixture := loadOpcodePlanFixture(t)
	keys := make(map[string]string, len(fixture.CoalesceHashCases))

	for _, tc := range fixture.CoalesceHashCases {
		t.Run(tc.Name, func(t *testing.T) {
			key := computeParallelCoalesceKey(tc.RunID, tc.StepIDs)
			require.Equal(t, tc.ExpectedKey, key)
			keys[tc.Name] = key
		})
	}

	for _, tc := range fixture.CoalesceHashCases {
		if tc.SameKeyAs != "" {
			require.Equal(t, keys[tc.SameKeyAs], keys[tc.Name], tc.Name)
		}
		if tc.DifferentKeyFrom != "" {
			require.NotEqual(t, keys[tc.DifferentKeyFrom], keys[tc.Name], tc.Name)
		}
	}
}

func TestOpcodePlanDiscoveryJobIDKnownAnswer(t *testing.T) {
	fixture := loadOpcodePlanFixture(t)
	var tc *coalesceHashCase
	for i := range fixture.CoalesceHashCases {
		if fixture.CoalesceHashCases[i].ExpectedDiscoveryJobID != "" {
			tc = &fixture.CoalesceHashCases[i]
			break
		}
	}
	require.NotNil(t, tc)

	runID, err := ulid.Parse(tc.RunID)
	require.NoError(t, err)
	key := computeParallelCoalesceKey(tc.RunID, tc.StepIDs)
	runCtx := &mockRunContext{md: sv2.Metadata{
		ID:     sv2.ID{RunID: runID, FunctionID: uuid.New()},
		Config: *sv2.InitConfig(&sv2.Config{RequestVersion: 2}),
	}}
	q := &stubQueue{}
	e := &executor{
		queue:          q,
		log:            logger.From(context.Background()),
		tracerProvider: tracing.NewNoopTracerProvider(),
	}
	coalesceKey := &key
	edge := queue.PayloadEdge{Edge: inngest.Edge{Incoming: "step"}}
	err = e.maybeEnqueueDiscoveryStep(
		context.Background(), runCtx, state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "step-a"},
		edge, "group-1", false, coalesceKey,
	)
	require.NoError(t, err)
	require.Len(t, q.enqueued, 1)
	require.NotNil(t, q.enqueued[0].JobID)
	require.Equal(t, tc.ExpectedDiscoveryJobID, *q.enqueued[0].JobID)
}

type opcodePlanRecordingRunService struct {
	sv2.RunService
	mu             sync.Mutex
	events         *opcodePlanEvents
	pendingCalls   [][]string
	updates        []sv2.MutableConfig
	saveSteps      int
	saveDefers     int
	savePendingErr error
}

func (s *opcodePlanRecordingRunService) UpdateMetadata(_ context.Context, _ sv2.ID, update sv2.MutableConfig) error {
	s.mu.Lock()
	s.updates = append(s.updates, update)
	s.mu.Unlock()
	s.events.record("metadata_update")
	return nil
}

func (s *opcodePlanRecordingRunService) SavePending(_ context.Context, _ sv2.ID, pending []string) error {
	s.mu.Lock()
	s.pendingCalls = append(s.pendingCalls, append([]string(nil), pending...))
	err := s.savePendingErr
	s.mu.Unlock()
	if err != nil {
		s.events.record("save_pending_failed")
		return err
	}
	s.events.record("save_pending_completed")
	return nil
}

func (s *opcodePlanRecordingRunService) SaveStep(_ context.Context, _ sv2.ID, _ string, _ []byte) (bool, error) {
	s.mu.Lock()
	s.saveSteps++
	s.mu.Unlock()
	s.events.record("handler:save_step")
	return false, nil
}

func (s *opcodePlanRecordingRunService) SaveDefer(_ context.Context, _ sv2.ID, _ sv2.Defer) error {
	s.mu.Lock()
	s.saveDefers++
	s.mu.Unlock()
	s.events.record("handler:save_defer")
	return nil
}

func (s *opcodePlanRecordingRunService) snapshot() ([][]string, []sv2.MutableConfig, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pendingCalls := make([][]string, len(s.pendingCalls))
	for i, pending := range s.pendingCalls {
		pendingCalls[i] = append([]string(nil), pending...)
	}
	return pendingCalls, append([]sv2.MutableConfig(nil), s.updates...), s.saveSteps, s.saveDefers
}

type opcodePlanEvents struct {
	mu     sync.Mutex
	values []string
}

func (r *opcodePlanEvents) record(value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values = append(r.values, value)
}

func (r *opcodePlanEvents) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.values...)
}

type opcodePlanRecordingQueue struct {
	queue.Queue
	mu       sync.Mutex
	events   *opcodePlanEvents
	enqueued []queue.Item
}

func (q *opcodePlanRecordingQueue) Enqueue(_ context.Context, item queue.Item, _ time.Time, _ queue.EnqueueOpts) error {
	q.events.record("handler:queue_enqueue")
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueued = append(q.enqueued, item)
	return nil
}

func (q *opcodePlanRecordingQueue) items() []queue.Item {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]queue.Item(nil), q.enqueued...)
}

type opcodePlanStepScheduledRecorder struct {
	execution.SyncLifecycleListener
	events *opcodePlanEvents
}

func (r *opcodePlanStepScheduledRecorder) OnStepScheduled(context.Context, sv2.Metadata, queue.Item, *string, time.Time) {
	r.events.record("handler:on_step_scheduled")
}

func requirePendingBeforeHandlerEffects(t *testing.T, events []string) {
	t.Helper()

	pendingSaved := false
	effects := 0
	for _, event := range events {
		if event == "save_pending_completed" {
			pendingSaved = true
			continue
		}
		if strings.HasPrefix(event, "handler:") {
			require.True(t, pendingSaved, "%s happened before SavePending completed: %v", event, events)
			effects++
		}
	}
	require.Positive(t, effects, "expected the handler to produce observable effects")
}

func TestOpcodePlanCompatibilityAtResponseHandler(t *testing.T) {
	fixture := loadOpcodePlanFixture(t)
	savePendingFailure := errors.New("fixture SavePending failure")
	for _, tc := range fixture.Cases {
		if !tc.HandlerCase {
			continue
		}
		t.Run(tc.Name, func(t *testing.T) {
			runID, err := ulid.Parse(tc.RunID)
			require.NoError(t, err)
			wsID, fnID, accountID := uuid.New(), uuid.New(), uuid.New()
			md := sv2.Metadata{
				ID: sv2.ID{RunID: runID, FunctionID: fnID, Tenant: sv2.Tenant{EnvID: wsID, AccountID: accountID}},
				Config: *sv2.InitConfig(&sv2.Config{
					RequestVersion: tc.StoredRequestVersion,
					ForceStepPlan:  tc.ForceStepPlan,
				}),
			}
			ops := tc.Input
			resp := &state.DriverResponse{Generator: ops, RequestVersion: tc.ResponseRequestVersion}
			events := &opcodePlanEvents{}
			service := &opcodePlanRecordingRunService{
				events: events,
			}
			if tc.FailSavePending {
				service.savePendingErr = savePendingFailure
			}
			q := &opcodePlanRecordingQueue{events: events}
			e := &executor{
				smv2:           service,
				queue:          q,
				log:            logger.From(context.Background()),
				tracerProvider: tracing.NewNoopTracerProvider(),
			}
			if len(tc.Input) > 0 {
				allPlanned := true
				for _, op := range tc.Input {
					allPlanned = allPlanned && op.Op == enums.OpcodeStepPlanned
				}
				if allPlanned {
					e.syncLifecycles = []execution.SyncLifecycleListener{
						&opcodePlanStepScheduledRecorder{events: events},
					}
				}
			}
			edge := inngest.Edge{Incoming: "step"}
			i := &runInstance{
				md:   md,
				edge: edge,
				resp: resp,
				item: queue.Item{Payload: queue.PayloadEdge{Edge: edge}},
			}

			err = e.HandleGeneratorResponse(context.Background(), i, resp)
			if tc.FailSavePending {
				require.ErrorIs(t, err, savePendingFailure)
			} else if tc.Expected.HandlerError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			pendingCalls, metadataUpdates, saveSteps, saveDefers := service.snapshot()
			if tc.Expected.ShouldCoalesce {
				require.Equal(t, [][]string{tc.Expected.NonLazyIDs}, pendingCalls)
			} else {
				require.Empty(t, pendingCalls)
			}
			updates := make([]opcodePlanMetadataUpdate, 0, len(metadataUpdates))
			for _, update := range metadataUpdates {
				updates = append(updates, opcodePlanMetadataUpdate{
					RequestVersion: update.RequestVersion,
					ForceStepPlan:  update.ForceStepPlan,
				})
			}
			require.Equal(t, tc.Expected.MetadataUpdates, updates)
			items := q.items()
			require.Len(t, items, tc.Expected.HandlerEnqueues)
			for _, item := range items {
				if tc.Expected.ShouldCoalesce && !tc.FailSavePending {
					require.NotNil(t, item.ParallelCoalesceKey)
					require.Equal(t, tc.Expected.CoalesceKey, *item.ParallelCoalesceKey)
				} else {
					require.Nil(t, item.ParallelCoalesceKey)
				}
			}

			if tc.Expected.ShouldCoalesce && len(tc.Expected.MetadataUpdates) == 0 && !tc.FailSavePending {
				requirePendingBeforeHandlerEffects(t, events.snapshot())
			}
			if tc.FailSavePending {
				require.Empty(t, items)
				require.Zero(t, saveSteps)
				require.Zero(t, saveDefers)
				for _, event := range events.snapshot() {
					require.NotContains(t, event, "handler:", "handler effect occurred after SavePending failed")
				}
			}
		})
	}
}
