package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type continuationOp struct {
	Op          enums.Opcode `json:"op"`
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	DisplayName *string      `json:"displayName"`
	Data        string       `json:"data"`
	Options     string       `json:"options"`
	Error       bool         `json:"error"`
	UserNoRetry bool         `json:"userNoRetry"`
	Mode        string       `json:"mode"`
	AI          bool         `json:"ai"`
	Prepared    string       `json:"prepared"` // Rust effect observation, never a Go answer.
	Pending     bool         `json:"pending"`
	SaveError   string       `json:"saveError"`
}

type continuationRecipe struct {
	continuationPayload
	AtNS   int64  `json:"atNs"`
	Branch int    `json:"branch"`
	Group  string `json:"group"`
}

type continuationScheduled struct {
	Branch  int                  `json:"branch"`
	Name    *string              `json:"name"`
	Group   string               `json:"group"`
	Attempt int                  `json:"attempt"`
	Payload *continuationPayload `json:"payload"`
}

// A callback carries the queue item, without the separately supplied enqueue time.
type continuationPayload struct {
	Kind                      string `json:"kind"`
	JobID                     string `json:"jobId"`
	Incoming                  string `json:"incoming"`
	Outgoing                  string `json:"outgoing"`
	IncomingGeneratorStep     string `json:"incomingGeneratorStep"`
	IncomingGeneratorStepName string `json:"incomingGeneratorStepName"`
	Mode                      string `json:"mode"`
	CoalesceKey               string `json:"coalesceKey"`
	Attempt                   int    `json:"attempt"`
	MaxAttempts               int    `json:"maxAttempts"`
	Priority                  int64  `json:"priority"`
}

type continuationExpected struct {
	Scheduled        []continuationScheduled `json:"scheduled"`
	Events           []string                `json:"events"`
	Before           [][2]string             `json:"before"`
	Writes           map[string]string       `json:"writes"`
	Recipes          []continuationRecipe    `json:"recipes"`
	Metadata         []sv2.MutableConfig     `json:"metadata"`
	Pending          [][]string              `json:"pending"`
	Error            string                  `json:"error"`
	ErrorContext     string                  `json:"errorContext"`
	Policy           string                  `json:"policy"`
	LifecycleNS      []int64                 `json:"lifecycleNs"`
	CallbackNS       []int64                 `json:"callbackNs"`
	HandledAtNS      *int64                  `json:"handledAtNs"`
	ClockCount       *int                    `json:"clockCount"`
	AlwaysRetryable  bool                    `json:"alwaysRetryable"`
	AfterEnqueueSpan string                  `json:"afterEnqueueSpan"`
}

type continuationCase struct {
	Name                string               `json:"name"`
	Ops                 []continuationOp     `json:"ops"`
	StoredVersion       *int                 `json:"storedVersion"`
	ResponseVersion     *int                 `json:"responseVersion"`
	Attempt             *int                 `json:"attempt"`
	MaxAttempts         *int                 `json:"maxAttempts"`
	Force               bool                 `json:"force"`
	NoRetry             bool                 `json:"noRetry"`
	ResponseUserNoRetry bool                 `json:"responseUserNoRetry"`
	ContextMode         string               `json:"contextMode"`
	ContextCoalesceKey  *string              `json:"contextCoalesceKey"`
	RetryAtNS           *int64               `json:"retryAtNs"`
	MetadataError       string               `json:"metadataError"`
	PendingError        string               `json:"pendingError"`
	ResetError          string               `json:"resetError"`
	QueueError          string               `json:"queueError"`
	Repeat              int                  `json:"repeat"`
	Tick                bool                 `json:"tick"`
	AdvanceMetadataNS   int64                `json:"advanceMetadataNs"`
	StateSizeLimit      int                  `json:"stateSizeLimit"`
	Expected            continuationExpected `json:"expected"`
}

// All decisions execute in the real Go owner. Recorders supply independent port
// outcomes; they never read Expected or implement a continuation on Go's behalf.
func TestResponseContinuationContract(t *testing.T) {
	runResponseContinuationContract(t, func(ctx context.Context, e *executor, i *runInstance, resp *state.DriverResponse) error {
		return e.HandleGeneratorResponse(ctx, i, resp)
	})
}

func runResponseContinuationContract(t *testing.T, run func(context.Context, *executor, *runInstance, *state.DriverResponse) error) {
	data, err := os.ReadFile("testdata/compatibility/response-continuation-v1.json")
	require.NoError(t, err)
	var fixture struct {
		SchemaVersion int    `json:"schemaVersion"`
		Contract      string `json:"contract"`
		Source        struct {
			Commit string `json:"commit"`
		} `json:"source"`
		Defaults struct {
			StoredVersion   int    `json:"storedVersion"`
			ResponseVersion int    `json:"responseVersion"`
			Attempt         int    `json:"attempt"`
			MaxAttempts     int    `json:"maxAttempts"`
			RunID           string `json:"runId"`
			IdempotencyKey  string `json:"idempotencyKey"`
			BaseTime        string `json:"baseTime"`
		} `json:"defaults"`
		Cases []continuationCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	require.Equal(t, "response-continuation-v1", fixture.Contract)
	require.Equal(t, "327acb5f22e4499cdfc4363b4465ce5ebb58d88d", fixture.Source.Commit)
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var ops []*state.GeneratorOpcode
				for _, o := range tc.Ops {
					opts := map[string]any{}
					if o.Mode != "" {
						opts["parallelMode"] = o.Mode
					}
					if o.AI {
						opts["type"] = "step.ai.wrap"
					}
					g := &state.GeneratorOpcode{Op: o.Op, ID: o.ID, Name: o.Name, DisplayName: o.DisplayName, Opts: opts}
					if o.Options != "" {
						require.True(t, json.Valid([]byte(o.Options)))
						g.Opts = json.RawMessage(o.Options)
					}
					if o.Data != "" {
						g.Data = json.RawMessage(o.Data)
					}
					if o.Error {
						g.Error = &state.UserError{Name: "E", Message: "x", NoRetry: o.UserNoRetry}
					}
					ops = append(ops, g)
				}
				if tc.Repeat > 0 {
					ops = slices.Repeat(ops, tc.Repeat)
				}
				e, i, s, q, events := newStagedBatch(t, ops...)
				base, parseErr := time.Parse(time.RFC3339Nano, fixture.Defaults.BaseTime)
				require.NoError(t, parseErr)
				i.md.Config.RequestVersion = fixture.Defaults.StoredVersion
				i.resp.RequestVersion = fixture.Defaults.ResponseVersion
				i.item.Attempt = fixture.Defaults.Attempt
				maxAttempts := fixture.Defaults.MaxAttempts
				i.item.MaxAttempts = &maxAttempts
				i.md.ID.RunID = ulid.MustParse(fixture.Defaults.RunID)
				functionID, idempotency, ok := strings.Cut(fixture.Defaults.IdempotencyKey, ":")
				require.True(t, ok)
				i.md.ID.FunctionID = uuid.MustParse(functionID)
				i.md.Config.Idempotency = idempotency
				clock := &continuationClock{Clock: clockwork.NewFakeClockAt(base), base: base, tick: tc.Tick}
				e.clock = clock
				if tc.StateSizeLimit != 0 {
					e.stateSizeLimit = func(sv2.ID) int { return tc.StateSizeLimit }
				}
				listener := &continuationLifecycle{stagedBatchLifecycle: stagedBatchLifecycle{events: events}, base: base}
				listener.ops = tc.Ops
				async := &continuationAsyncLifecycle{}
				e.lifecycles = []execution.LifecycleListener{async}
				e.syncLifecycles = []execution.SyncLifecycleListener{listener}
				var handlingTracer *recordingTracerProvider
				if tc.Expected.HandledAtNS != nil || tc.StateSizeLimit != 0 {
					handlingTracer = newRecordingTracerProvider()
					e.tracerProvider = handlingTracer
				}
				if tc.Expected.AfterEnqueueSpan != "" {
					e.tracerProvider = tracing.NewOtelTracerProvider(&continuationExporter{q: q, events: events}, 0)
				}
				if tc.StoredVersion != nil {
					i.md.Config.RequestVersion = *tc.StoredVersion
				}
				if tc.ResponseVersion != nil {
					i.resp.RequestVersion = *tc.ResponseVersion
				}
				if tc.Attempt != nil {
					i.item.Attempt = *tc.Attempt
				}
				if tc.MaxAttempts != nil {
					i.item.MaxAttempts = tc.MaxAttempts
				}
				i.md.Config.ForceStepPlan = tc.Force
				i.item.ParallelMode = continuationMode(tc.ContextMode)
				i.item.ParallelCoalesceKey = tc.ContextCoalesceKey
				i.resp.NoRetry = tc.NoRetry
				if tc.ResponseUserNoRetry {
					i.resp.UserError = &state.UserError{NoRetry: true}
				}
				if tc.RetryAtNS != nil {
					at := base.Add(time.Duration(*tc.RetryAtNS))
					i.resp.RetryAt = &at
				}
				faults := map[string]error{
					"already-always":       queue.AlwaysRetryError(errors.New("contract already always")),
					"ordinary":             errors.New("contract ordinary"),
					"overflow":             fmt.Errorf("contract store: %w", state.ErrStateOverflowed),
					"preparation-overflow": state.ErrStateOverflowed,
					"wrapped-duplicate":    fmt.Errorf("contract store: %w", state.ErrDuplicateResponse),
					"idempotent":           state.ErrIdempotentResponse,
					"metadata":             errors.New("contract metadata"), "pending": errors.New("contract pending"),
					"reset": errors.New("contract reset"), "queue-duplicate": queue.ErrQueueItemExists,
					"handled": ErrHandledStepError, "function-overflow": state.ErrFunctionOverflowed,
				}
				for _, o := range tc.Ops {
					s.outcomes[o.ID] = stagedSaveOutcome{pending: o.Pending, err: faults[o.SaveError]}
				}
				s.metadataErr = faults[tc.MetadataError]
				if tc.ResetError != "" {
					s.metadataErr = faults[tc.ResetError]
				}
				s.savePendingErr = faults[tc.PendingError]
				s.afterMetadata = func() { clock.advance(time.Duration(tc.AdvanceMetadataNS)) }
				q.err = faults[tc.QueueError]
				originalAttempt := i.item.Attempt
				originalVersion, originalForce := i.md.Config.RequestVersion, i.md.Config.ForceStepPlan
				i.item.GroupID = "original-group"
				result := run(state.WithGroupID(context.Background(), "original-group"), e, i, i.resp)
				// Wait for every goroutine in this isolated bubble, including async
				// listeners. Missing callbacks fail the literal count assertion;
				// no expected-count WaitGroup or timed absence window is needed.
				synctest.Wait()
				assertContinuationError(t, tc, result, faults)
				require.Equal(t, originalAttempt, i.item.Attempt, "each sibling increments its branch copy")
				require.Equal(t, originalVersion, i.md.Config.RequestVersion)
				require.Equal(t, originalForce, i.md.Config.ForceStepPlan)
				require.Equal(t, tc.Expected.Writes, s.writes)
				pending, updates, _, _ := s.snapshot()
				require.Equal(t, tc.Expected.Pending, nonNil(pending))
				require.Equal(t, tc.Expected.Metadata, nonNil(updates))
				observed := events.snapshot()
				filtered := slices.DeleteFunc(slices.Clone(observed), func(e string) bool { return strings.HasPrefix(e, "span:") })
				require.ElementsMatch(t, tc.Expected.Events, filtered)
				for _, pair := range tc.Expected.Before {
					requirePartialOrder(t, observed, pair[0], pair[1])
				}
				groups := &continuationGroups{byValue: map[string]string{}, bySite: map[string]string{}}
				for branch, group := range listener.historyGroups {
					site := "original"
					if len(ops) > 1 {
						site = fmt.Sprintf("history:%d", branch)
					}
					groups.bind(t, site, group)
				}
				actualAsync := continuationCallbacks(t, tc, i.item, async.calls, groups)
				actualSync := continuationCallbacks(t, tc, i.item, listener.calls, groups)
				require.ElementsMatch(t, tc.Expected.Scheduled, actualAsync, "async delivery, payload and suppression")
				require.ElementsMatch(t, tc.Expected.Scheduled, actualSync, "sync delivery and UUID bindings")
				var recipes []continuationRecipe
				for _, call := range q.calls {
					requireStagedQueueBindings(t, call)
					require.Equal(t, queue.EnqueueOpts{}, call.opts)
					branch := continuationQueueBranch(t, tc, call.item, groups)
					site := "discovery"
					if tc.Ops[branch].Op == enums.OpcodeStepPlanned {
						site = "planned"
					}
					group := fmt.Sprintf("%s:%d", site, branch)
					groups.bind(t, group, call.item.GroupID)
					recipes = append(recipes, continuationRecipe{continuationPayload: continuationPayloadOf(call.item), AtNS: call.at.Sub(base).Nanoseconds(), Branch: branch, Group: group})
				}
				require.ElementsMatch(t, tc.Expected.Recipes, recipes)
				if tc.Expected.LifecycleNS != nil {
					require.ElementsMatch(t, tc.Expected.LifecycleNS, listener.lifecycleNS)
				}
				if tc.Expected.CallbackNS != nil {
					require.ElementsMatch(t, tc.Expected.CallbackNS, listener.callbackNS)
				}
				if tc.Expected.ClockCount != nil {
					require.Equal(t, *tc.Expected.ClockCount, clock.count)
				}
				if span := tc.Expected.AfterEnqueueSpan; span != "" {
					if span == "drop" {
						// The execution span processor filters dropped spans before
						// export. This observes suppression, not a private Drop call.
						require.NotContains(t, observed, "span:send")
					} else {
						require.Contains(t, observed, "span:send")
						enqueue := "handler:enqueue:" + ops[0].ID
						if ops[0].Op == enums.OpcodeStepPlanned {
							enqueue = "handler:enqueue:previous"
						}
						requirePartialOrder(t, observed, enqueue, "span:send")
						for _, event := range observed {
							if strings.HasPrefix(event, "scheduled:") {
								requirePartialOrder(t, observed, "span:send", event)
							}
						}
					}
				}
				if tc.StateSizeLimit != 0 {
					require.Empty(t, handlingTracer.createCalls, "size validation fails before step span preparation")
				}
				if tc.Expected.HandledAtNS != nil {
					var handled []int64
					for _, call := range handlingTracer.createCalls {
						if call.name == meta.SpanNameStep {
							at, ok := meta.GetAttr(call.opts.Attributes, meta.Attrs.QueuedAt)
							require.True(t, ok)
							handled = append(handled, at.Sub(base).Nanoseconds())
						}
					}
					require.Equal(t, []int64{*tc.Expected.HandledAtNS}, handled)
				}
			})
		})
	}
}

func nonNil[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}
func continuationMode(v string) enums.ParallelMode {
	switch v {
	case "race":
		return enums.ParallelModeRace
	case "wait":
		return enums.ParallelModeWait
	default:
		return enums.ParallelModeNone
	}
}
func requirePartialOrder(t *testing.T, events []string, a, b string) {
	t.Helper()
	firstB := slices.Index(events, b)
	lastA := -1
	for j, event := range events {
		if event == a {
			lastA = j
		}
	}
	require.GreaterOrEqual(t, lastA, 0)
	require.Greater(t, firstB, lastA, "%s before %s: %v", a, b, events)
}
func assertContinuationError(t *testing.T, tc continuationCase, err error, faults map[string]error) {
	t.Helper()
	if tc.Expected.Error == "none" {
		require.NoError(t, err)
		return
	}
	require.Error(t, err)
	prefixes := map[string]string{"original": "", "metadata": "error updating function metadata: ", "pending": "error saving pending steps: ", "reset": "error resetting force step plan: ", "discovery": "error enqueueing discovery step: "}
	prefix, known := prefixes[tc.Expected.ErrorContext]
	require.True(t, known)
	require.True(t, strings.HasPrefix(err.Error(), prefix), "original error wrapper context: %v", err)
	matched := false
	for _, symbol := range strings.Split(tc.Expected.Error, "|") {
		original := faults[symbol]
		require.NotNil(t, original)
		if errors.Is(err, original) || strings.Contains(err.Error(), original.Error()) {
			matched = true
		}
	}
	require.True(t, matched, "unexpected original error: %v", err)
	switch tc.Expected.Policy {
	case "always":
		require.True(t, queue.IsAlwaysRetryable(err))
	case "never":
		require.False(t, queue.ShouldRetry(err, 0, 3))
		require.Nil(t, queue.AsRetryAtError(err))
	case "at":
		at := queue.AsRetryAtError(err)
		require.NotNil(t, at)
		base := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
		require.Equal(t, base.Add(time.Duration(*tc.RetryAtNS)), *at.NextRetryAt())
	default:
		require.Equal(t, tc.Expected.AlwaysRetryable, queue.IsAlwaysRetryable(err))
		require.Nil(t, queue.AsRetryAtError(err))
		require.True(t, queue.ShouldRetry(err, 0, 3))
	}
	if tc.Expected.Error == "wrapped-duplicate" || tc.Expected.Error == "overflow" && tc.Expected.Policy == "original" {
		require.Same(t, faults[tc.Expected.Error], err)
	}
	if tc.Expected.Error == "preparation-overflow" {
		requireContinuationPreparationOverflow(t, err)
	}
	if tc.Expected.Error == "already-always" {
		require.Equal(t, faults[tc.Expected.Error], err, "retain the original value wrapper and underlying identity")
	}
}

// The real preparation owner creates this standard wrapper. Require it to be
// returned directly, with the original overflow sentinel and no response retry
// wrapper or reconstruction, even when NoRetry and RetryAt were supplied.
func requireContinuationPreparationOverflow(t *testing.T, err error) {
	t.Helper()
	require.IsType(t, &state.WrappedStandardError{}, err)
	require.Same(t, state.ErrStateOverflowed, errors.Unwrap(err))
	standard := err.(*state.WrappedStandardError)
	require.Equal(t, "InngestErrStateOverflowed", standard.Name)
	require.Equal(t, "The function run exceeded the state size limit of 12 bytes.", standard.Message)
}

type continuationClock struct {
	clockwork.Clock
	mu     sync.Mutex
	base   time.Time
	tick   bool
	count  int
	offset time.Duration
}

func (c *continuationClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.base.Add(c.offset)
	if c.tick {
		at = at.Add(time.Duration(c.count) * time.Second)
	}
	c.count++
	return at
}
func (c *continuationClock) advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.offset += d }

type continuationLifecycle struct {
	stagedBatchLifecycle
	mu                      sync.Mutex
	base                    time.Time
	lifecycleNS, callbackNS []int64
	ops                     []continuationOp
	historyGroups           map[int]string
	calls                   []continuationScheduledCall
}

func (l *continuationLifecycle) OnStepRunFinished(ctx context.Context, md sv2.Metadata, item queue.Item, edge inngest.Edge, g state.GeneratorOpcode, at time.Time) {
	l.stagedBatchLifecycle.OnStepRunFinished(ctx, md, item, edge, g, at)
	l.record(g, item.GroupID, at)
}
func (l *continuationLifecycle) OnStepRunFailed(ctx context.Context, md sv2.Metadata, item queue.Item, edge inngest.Edge, g state.GeneratorOpcode, status enums.StepStatus, attempt int, at time.Time) {
	l.stagedBatchLifecycle.OnStepRunFailed(ctx, md, item, edge, g, status, attempt, at)
	l.record(g, item.GroupID, at)
}
func (l *continuationLifecycle) record(g state.GeneratorOpcode, group string, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lifecycleNS = append(l.lifecycleNS, at.Sub(l.base).Nanoseconds())
	if l.historyGroups == nil {
		l.historyGroups = map[int]string{}
	}
	for index, op := range l.ops {
		if op.ID == g.ID && op.Name == g.Name {
			l.historyGroups[index] = group
			return
		}
	}
	panic("unbound lifecycle branch")
}
func (l *continuationLifecycle) OnStepScheduled(ctx context.Context, md sv2.Metadata, item queue.Item, name *string, at time.Time) {
	l.stagedBatchLifecycle.OnStepScheduled(ctx, md, item, name, at)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.callbackNS = append(l.callbackNS, at.Sub(l.base).Nanoseconds())
	l.calls = append(l.calls, continuationScheduledCall{item: item, name: continuationName(name), contextGroup: state.GroupIDFromContext(ctx)})
}

type continuationScheduledCall struct {
	item         queue.Item
	name         *string
	contextGroup string
}

func continuationName(name *string) *string {
	if name == nil {
		return nil
	}
	value := *name
	return &value
}

type continuationAsyncLifecycle struct {
	execution.NoopLifecyceListener
	mu    sync.Mutex
	calls []continuationScheduledCall
}

func (l *continuationAsyncLifecycle) OnStepScheduled(ctx context.Context, _ sv2.Metadata, item queue.Item, name *string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, continuationScheduledCall{item: item, name: continuationName(name), contextGroup: state.GroupIDFromContext(ctx)})
}

// Go generates its own UUIDs. Normalize observable relationships only; the
// fixture's uuidSites requests are asserted by the suspended Rust adapter.
type continuationGroups struct{ byValue, bySite map[string]string }

func (g *continuationGroups) bind(t *testing.T, site, value string) {
	t.Helper()
	if site == "original" {
		require.Equal(t, "original-group", value)
		return
	}
	id, err := uuid.Parse(value)
	require.NoError(t, err, "fresh UUID for %s", site)
	require.NotEqual(t, uuid.Nil, id)
	if previous, ok := g.bySite[site]; ok {
		require.Equal(t, previous, value, "same allocation at %s", site)
	}
	if previous, ok := g.byValue[value]; ok {
		require.Equal(t, previous, site, "independent UUID allocations")
	}
	g.byValue[value], g.bySite[site] = site, value
}

func continuationPayloadOf(item queue.Item) continuationPayload {
	edge := item.Payload.(queue.PayloadEdge).Edge
	ck, job := "", ""
	if item.ParallelCoalesceKey != nil {
		ck = *item.ParallelCoalesceKey
	}
	if item.JobID != nil {
		job = *item.JobID
	}
	return continuationPayload{Kind: item.Kind, JobID: job, Incoming: edge.Incoming, Outgoing: edge.Outgoing, IncomingGeneratorStep: edge.IncomingGeneratorStep, IncomingGeneratorStepName: edge.IncomingGeneratorStepName, Mode: strings.ToLower(item.ParallelMode.String()), CoalesceKey: ck, Attempt: item.Attempt, MaxAttempts: *item.MaxAttempts, Priority: *item.PriorityFactor}
}
func continuationCallbackBranch(t *testing.T, tc continuationCase, call continuationScheduledCall) int {
	t.Helper()
	edge := call.item.Payload.(queue.PayloadEdge).Edge
	var candidates []int
	for branch, op := range tc.Ops {
		matches := false
		switch {
		case call.item.Kind == "": // Retry uses the original lifecycle item.
			matches = op.Op == enums.OpcodeStepError && call.name != nil && op.Name == *call.name
		case edge.IncomingGeneratorStep != "":
			matches = op.Op == enums.OpcodeStepPlanned && op.ID == edge.IncomingGeneratorStep && call.name != nil && op.Name == *call.name
		default:
			matches = op.ID == edge.Outgoing
		}
		if matches {
			candidates = append(candidates, branch)
		}
	}
	require.Len(t, candidates, 1, "callback must identify an original branch: %+v", call)
	return candidates[0]
}
func continuationQueueBranch(t *testing.T, tc continuationCase, item queue.Item, groups *continuationGroups) int {
	t.Helper()
	if site, ok := groups.byValue[item.GroupID]; ok {
		for branch := range tc.Ops {
			if site == fmt.Sprintf("planned:%d", branch) || site == fmt.Sprintf("discovery:%d", branch) {
				return branch
			}
		}
		t.Fatalf("queue item bound to history UUID: %s", site)
	}
	edge := item.Payload.(queue.PayloadEdge).Edge
	var candidates []int
	for branch, op := range tc.Ops {
		if op.ID == edge.Outgoing && op.Op != enums.OpcodeStepPlanned || op.ID == edge.IncomingGeneratorStep && op.Op == enums.OpcodeStepPlanned {
			candidates = append(candidates, branch)
		}
	}
	require.Len(t, candidates, 1, "queue branch must be observable without sibling ordering")
	return candidates[0]
}
func continuationCallbacks(t *testing.T, tc continuationCase, original queue.Item, calls []continuationScheduledCall, groups *continuationGroups) []continuationScheduled {
	t.Helper()
	var views []continuationScheduled
	for _, call := range calls {
		branch := continuationCallbackBranch(t, tc, call)
		group := "original"
		var payload *continuationPayload
		if call.item.Kind == "" {
			if len(tc.Ops) > 1 {
				group = fmt.Sprintf("history:%d", branch)
			}
			want := original
			want.Attempt++
			want.GroupID = call.item.GroupID
			require.Equal(t, want, call.item, "retry preserves original item and increments only branch attempt")
			require.Equal(t, "original-group", call.contextGroup, "retry retains incoming context")
		} else {
			site := "discovery"
			if tc.Ops[branch].Op == enums.OpcodeStepPlanned {
				site = "planned"
			}
			group = fmt.Sprintf("%s:%d", site, branch)
			require.Equal(t, call.item.GroupID, call.contextGroup)
			value := continuationPayloadOf(call.item)
			payload = &value
		}
		groups.bind(t, group, call.item.GroupID)
		views = append(views, continuationScheduled{Branch: branch, Name: call.name, Group: group, Attempt: call.item.Attempt, Payload: payload})
	}
	return views
}

type continuationExporter struct {
	q      *stagedBatchQueue
	events *opcodePlanEvents
}

func (e *continuationExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.q.mu.Lock()
	enqueued := len(e.q.calls) > 0
	e.q.mu.Unlock()
	if enqueued {
		for _, span := range spans {
			if span.Name() != meta.SpanNameStep && span.Name() != meta.SpanNameStepDiscovery {
				continue
			}
			action := "send"
			for _, a := range span.Attributes() {
				if string(a.Key) == meta.Attrs.DropSpan.Key() && a.Value.AsBool() {
					action = "drop"
				}
			}
			e.events.record("span:" + action)
		}
	}
	return nil
}
func (*continuationExporter) Shutdown(context.Context) error { return nil }

// This boundary supports an observable release via the existing RunContext port.
// HandleGeneratorResponse's concrete runInstance cannot install a queue-private
// release function; no test-only production setter is introduced for it.
func TestResponseContinuationPreparationFailureBeforeRelease(t *testing.T) {
	g := state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "oversized", Data: json.RawMessage(`null`)}
	e, i, s, q, events := newStagedBatch(t, &g)
	e.stateSizeLimit = func(sv2.ID) int { return 12 }
	err := e.HandleGenerator(context.Background(), &stagedReleaseContext{runInstance: i, events: events}, g)
	requireContinuationPreparationOverflow(t, err)
	require.Empty(t, s.writes)
	require.Empty(t, q.calls)
	require.Empty(t, events.snapshot(), "failed preparation must not save, notify lifecycle, release capacity or enqueue")
}

func TestResponseContinuationResetFailureAfterRelease(t *testing.T) {
	g := state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}
	e, i, s, q, events := newStagedBatch(t, &g)
	i.md.Config.ForceStepPlan = true
	reset := errors.New("reset unavailable")
	s.metadataErr = reset
	err := e.HandleGenerator(context.Background(), &stagedReleaseContext{runInstance: i, events: events}, g)
	require.ErrorIs(t, err, reset)
	require.Empty(t, q.calls)
	require.Equal(t, []string{"handler:save:s", "finished:s", "release", "metadata_update"}, events.snapshot())
}

func TestResponseContinuationCompetingErrorsJoin(t *testing.T) {
	for _, winner := range []string{"a", "b"} {
		t.Run(winner, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, i, s, _, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "a"}, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "b"})
				i.md.Config.ForceStepPlan = true
				first, second := errors.New("first"), fmt.Errorf("second: %w", state.ErrStateOverflowed)
				s.outcomes["a"] = stagedSaveOutcome{err: first}
				s.outcomes["b"] = stagedSaveOutcome{err: second}
				gates := map[string]chan struct{}{"a": make(chan struct{}), "b": make(chan struct{})}
				releases := map[string]func(){"a": sync.OnceFunc(func() { close(gates["a"]) }), "b": sync.OnceFunc(func() { close(gates["b"]) })}
				entered := make(chan string, 2)
				done := make(chan error, 1)
				s.beforeSave = func(id string) { entered <- id; <-gates[id] }
				defer func() { releases["a"](); releases["b"](); synctest.Wait() }()
				go func() { done <- e.HandleGeneratorResponse(context.Background(), i, i.resp) }()
				synctest.Wait()
				require.Len(t, entered, 2)
				releases[winner]()
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("returned without full join: %v", err)
				default:
				}
				loser := "b"
				want := first
				if winner == "b" {
					loser = "a"
					want = second
				}
				releases[loser]()
				synctest.Wait()
				require.Same(t, want, <-done, "observed first error, without severity ranking")
				require.ElementsMatch(t, []string{"handler:save:a", "handler:save:b"}, events.snapshot())
			})
		})
	}
}
