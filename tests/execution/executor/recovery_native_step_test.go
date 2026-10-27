package executor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	executorpkg "github.com/inngest/inngest/pkg/execution/executor"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type nativeReplayFixture struct {
	Idempotency         string            `json:"idempotency"`
	OriginalJob         string            `json:"original_job"`
	DiscoveryJob        string            `json:"discovery_job"`
	DiscoveryLogicalJob string            `json:"discovery_logical_job"`
	Output              string            `json:"output"`
	StateKeys           map[string]string `json:"state_keys"`
	QueueKeys           map[string]string `json:"queue_keys"`
	Metadata            map[string]string `json:"metadata"`
	InitialStateSize    int               `json:"initial_state_size"`
	OutputBytes         int               `json:"output_bytes"`
	ImmediateScore      int64             `json:"immediate_score_ms"`
	Scenarios           []struct {
		Name               string   `json:"name"`
		Fault              string   `json:"fault"`
		Remaining          []string `json:"remaining"`
		ReplayJob          string   `json:"replay_job"`
		ReplayStackCurrent int      `json:"replay_stack_current"`
	} `json:"scenarios"`
}

// Replaces drained Go owners against surviving stores. No process crash, active
// lease recovery, fencing, exactly-once, or atomic SaveStep+enqueue claim follows.
func TestRecoveryReplaysNativeStepBaseline(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(recoveryFixtureDir, "native-boundary.json"))
	require.NoError(t, err)
	var f nativeReplayFixture
	require.NoError(t, json.Unmarshal(raw, &f))
	for _, tc := range f.Scenarios {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := logger.WithStdlib(context.Background(), logger.VoidLogger())
			v, err := queuehelper.StartValkey(t)
			require.NoError(t, err)
			directory := t.TempDir()
			now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
			retryAt := now.Add(10 * time.Minute)
			id := statev2.ID{RunID: ulid.MustParse("01K8F08000AAAAAAAAAAAAAAAA"), FunctionID: uuid.MustParse("11111111-2222-3333-4444-555555555555"), Tenant: statev2.Tenant{
				AccountID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), EnvID: uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), AppID: uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc"),
			}}
			scope := queue.Scope{AccountID: id.Tenant.AccountID, EnvID: id.Tenant.EnvID, FunctionID: id.FunctionID}
			keyQueues := os.Getenv("EXPERIMENTAL_KEY_QUEUES_ENABLE") == "true"
			t.Logf("key queues: %t", keyQueues)

			// This endpoint has no executor or state adapter. Every request missing the
			// literal checkpoint repeats INCR, with no dedupe or callback-count branch.
			effects := recoveryValkeyClient(t, v)
			requests := make(chan nativeReplayHTTPRequest, 10)
			atReplay := make(chan nativeReplayReadout, 10)
			checkpoint, terminal := recoveryFixture(t, "native-checkpoint.http"), recoveryFixture(t, "native-terminal.http")
			sdk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				var req struct {
					Steps map[string]json.RawMessage `json:"steps"`
				}
				if err != nil || json.Unmarshal(body, &req) != nil {
					http.Error(w, "invalid request", 400)
					return
				}
				select {
				case requests <- nativeReplayHTTPRequest{recoveryHTTPRequest: recoveryHTTPRequest{Body: body, Version: r.Header.Get("x-inngest-req-version"), StepID: r.URL.Query().Get("stepId")}, Header: r.Header.Clone()}:
				default:
					http.Error(w, "request flood", 429)
					return
				}
				response := checkpoint
				if saved, ok := req.Steps["checkpoint-output"]; ok && string(saved) == f.Output {
					// Read through a new raw connection before the terminal response permits
					// finalization to delete the checkpoint. This is an observation, not input.
					atReplay <- readNativeReplayStore(r.Context(), v, f)
					response = terminal
				} else {
					if err := effects.Do(r.Context(), effects.B().Incr().Key("fixture:external-effect").Build()).Error(); err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
				}
				for key, values := range response.Header {
					w.Header()[key] = values
				}
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(response.Body)
			}))
			t.Cleanup(sdk.Close)

			clockA := clockwork.NewFakeClockAt(now)
			a := newRecoveryRuntime(t, ctx, v, directory, clockA, retryAt, keyQueues)
			// A's real scanner may discover follow-up work immediately. Hold only its
			// discovery admission before the real Lease call, without mutating the item
			// or enqueue time. B has the ordinary, unwrapped shard and normal scanner.
			gate := &nativeReplayAdmission{QueueShard: a.registry.Primary()}
			require.NoError(t, a.registry.Add(gate))
			require.NoError(t, a.registry.SetPrimary(ctx, gate))
			zero := 0
			fn := inngest.Function{ID: id.FunctionID, FunctionVersion: 1, Name: "recovery-fixture", Slug: "recovery-fixture", Steps: []inngest.Step{{ID: "step", Name: "step", URI: sdk.URL, Retries: &zero}}}
			config, err := json.Marshal(fn)
			require.NoError(t, err)
			_, err = a.cqrs.UpsertApp(ctx, cqrs.UpsertAppParams{ID: id.Tenant.AppID, Name: "recovery-fixture"})
			require.NoError(t, err)
			_, err = a.cqrs.UpsertFunction(ctx, cqrs.UpsertFunctionParams{ID: fn.ID, AppID: id.Tenant.AppID, Name: fn.Name, Slug: fn.Slug, Config: string(config)})
			require.NoError(t, err)

			saves := &nativeReplaySave{RunService: a.state, observations: make(chan nativeReplaySaveObservation, 10), read: func(c context.Context) nativeReplayReadout { return readNativeReplayStore(c, v, f) }}
			beforeErr := errors.New("fixture: discovery enqueue unavailable before commit")
			fault := &nativeReplayDiscovery{Queue: a.queue, mode: tc.Fault, beforeErr: beforeErr, observations: make(chan nativeReplayEnqueueObservation, 10), read: func(c context.Context) nativeReplayReadout { return readNativeReplayStore(c, v, f) }, canceledClient: effects}
			a.makeExecutor(t, ctx, fault, executorpkg.WithStateManager(saves))
			idempotency := "sdk-response-recovery-v1"
			_, md, err := a.exec.Schedule(ctx, execution.ScheduleRequest{Function: fn, RunID: &id.RunID, At: &now, IdempotencyKey: &idempotency, AccountID: id.Tenant.AccountID, WorkspaceID: id.Tenant.EnvID, AppID: id.Tenant.AppID,
				Events: []event.TrackedEvent{event.NewBaseTrackedEventWithID(event.Event{ID: "source-event-v1", Name: "fixture/recovery", Timestamp: now.UnixMilli(), Data: map[string]any{"order": "order-1"}}, ulid.MustParse("01K8F08000BBBBBBBBBBBBBBBB"))},
			})
			require.NoError(t, err)
			require.Equal(t, id, md.ID)
			require.Equal(t, f.Idempotency, md.Config.Idempotency)
			precreated := readNativeReplayStore(ctx, v, f)
			require.NoError(t, precreated.Err)
			require.Equal(t, fmt.Sprint(f.InitialStateSize), precreated.Metadata["state_size"])
			require.Equal(t, "0", precreated.Metadata["metadata_size"])
			original, err := a.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
			require.NoError(t, err)
			require.Len(t, original, 1)
			require.Equal(t, f.OriginalJob, original[0].JobID)
			resultsA := a.start(t, ctx)
			recoveryEventually(t, clockA, func() bool { return len(resultsA) > 0 })
			resultA := <-resultsA
			t.Logf("A callback: %+v", resultA)
			require.Equal(t, f.OriginalJob, resultA.JobID)
			require.Zero(t, resultA.Attempt)
			observation := takeNativeReplayObservation(t, fault.observations)
			require.NoError(t, observation.Before.Err)
			t.Logf("pre-discovery: %+v", observation.Before)
			requireNativeReplayCheckpoint(t, f, observation.Before)
			require.Equal(t, f.DiscoveryLogicalJob, observation.LogicalJob)
			require.Equal(t, observation.Group, observation.ContextGroup)
			group, err := uuid.Parse(observation.Group)
			require.NoError(t, err)
			require.Equal(t, uuid.Version(4), group.Version())
			save := takeNativeReplayObservation(t, saves.observations)
			require.NoError(t, save.Err)
			require.Equal(t, id, save.ID)
			require.Equal(t, "checkpoint-output", save.Step)
			require.Equal(t, f.Output, save.Output)
			require.Equal(t, f.OriginalJob, save.Job)
			require.GreaterOrEqual(t, save.MetadataDelta, 0)
			require.Equal(t, fmt.Sprint(save.MetadataDelta), save.After.Metadata["metadata_size"])
			require.Equal(t, save.After.Metadata["metadata_size"], observation.Before.Metadata["metadata_size"])
			require.False(t, save.Pending)
			require.NoError(t, save.ContextErr)
			requireNativeReplayCheckpoint(t, f, save.After)
			if tc.Fault == "" {
				require.NoError(t, resultA.Err)
			} else {
				require.True(t, queue.IsAlwaysRetryable(resultA.Err))
				require.EqualError(t, resultA.Err, "error handling generator response: error enqueueing discovery step: "+observation.Returned.Error())
				if tc.Fault == "before" {
					require.Same(t, beforeErr, observation.Returned)
				} else {
					require.ErrorIs(t, observation.Returned, context.Canceled)
				}
			}
			if tc.Fault != "before" {
				require.NoError(t, observation.CommitErr, "must perform one real enqueue before suppressing its reply")
				requireNativeReplayCheckpoint(t, f, observation.After)
				require.Contains(t, observation.After.Items, f.DiscoveryJob)
				requireNativeReplayCarrier(t, observation.After.Items[f.DiscoveryJob], observation.Group)
			} else {
				require.NotContains(t, observation.Before.Items, f.DiscoveryJob)
			}
			require.NoError(t, observation.ContextErr, "the injected response loss must leave the actual caller live")
			initialRequest := takeNativeReplayObservation(t, requests)
			recoveryAssertRequest(t, "initial.request.json", initialRequest.recoveryHTTPRequest, f.OriginalJob)

			// Callback completion precedes durable dequeue/requeue. Do not drain until
			// the original is removed normally, or persisted at retryAt, attempt 0,
			// without an item lease. No captured item is used to produce future work.
			recoveryEventually(t, clockA, func() bool {
				jobs, e := a.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
				if e != nil || len(jobs) != len(tc.Remaining) {
					return false
				}
				ids := []string{}
				for _, job := range jobs {
					item := job.Raw.(*queue.QueueItem)
					if item.LeaseID != nil {
						return false
					}
					if job.JobID == f.OriginalJob && (item.AtMS != retryAt.UnixMilli() || item.Data.Attempt != 0) {
						return false
					}
					ids = append(ids, job.JobID)
				}
				slices.Sort(ids)
				return slices.Equal(tc.Remaining, ids)
			})
			require.Empty(t, fault.observations)
			require.Empty(t, saves.observations)
			saveMetadataSize := save.After.Metadata["metadata_size"]
			a.close(t, ctx)
			a = nil
			fault = nil
			saves = nil
			gate = nil

			// Every read here opens/closes a separate cache-disabled store connection.
			drained := readNativeReplayStore(ctx, v, f)
			requireNativeReplayCheckpoint(t, f, drained)
			require.Equal(t, saveMetadataSize, drained.Metadata["metadata_size"])
			t.Logf("A drained raw store: %+v", drained)
			requireNativeReplayJobs(t, f, drained, tc.Remaining, now, retryAt, keyQueues)

			clockB := clockwork.NewFakeClockAt(now)
			b := newRecoveryRuntime(t, ctx, v, directory, clockB, retryAt, keyQueues)
			b.makeExecutor(t, ctx, b.queue)
			loaded, err := b.loader.LoadFunction(ctx, id.Tenant.EnvID, id.FunctionID)
			require.NoError(t, err)
			require.Equal(t, fn, *loaded.Function)
			require.Equal(t, 1, loaded.Function.MaxAttempts())
			persisted := readRecoveryCheckpoint(ctx, b.state, id)
			require.NoError(t, persisted.Err)
			recoveryAssertJSON(t, "checkpoint.state.json", persisted.JSON)
			resultsB := b.start(t, ctx)
			// Advance past A's short scanner/partition leases, but keep an uncertain
			// acknowledgement's original retry in the future. The persisted discovery
			// must drive recovery in that case. The before-commit case uses the retry.
			if tc.Fault == "before" {
				clockB.Advance(retryAt.Sub(now))
			} else {
				clockB.Advance(time.Minute)
			}
			recoveryEventually(t, clockB, func() bool { return len(resultsB) > 0 })
			resultB := <-resultsB
			require.NoError(t, resultB.Err)
			require.Equal(t, tc.ReplayJob, resultB.JobID)
			require.Zero(t, resultB.Attempt)
			request := takeNativeReplayObservation(t, requests)
			recoveryAssertRequest(t, "replay.request.json", request.recoveryHTTPRequest, tc.ReplayJob, tc.ReplayStackCurrent)
			requireNativeReplayHTTPRequest(t, f, request, drained.Items[tc.ReplayJob])
			select {
			case replay := <-atReplay:
				requireNativeReplayCheckpoint(t, f, replay)
				require.Equal(t, saveMetadataSize, replay.Metadata["metadata_size"])
				require.Contains(t, replay.Items, tc.ReplayJob)
				t.Logf("B pre-terminal raw store: %+v", replay)
			default:
				t.Fatal("missing independent pre-finalization observation")
			}
			recoveryEventually(t, clockB, func() bool {
				jobs, e := b.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
				exists, ee := b.state.Exists(ctx, id)
				return e == nil && ee == nil && len(jobs) == 0 && !exists
			})
			b.close(t, ctx)
			b = nil
			final := readNativeReplayStore(ctx, v, f)
			require.NoError(t, final.Err)
			require.Empty(t, final.Items)
			require.Empty(t, final.Actions)
			require.Empty(t, final.Stack)
			require.Empty(t, final.Metadata)
			require.Empty(t, final.Pending)
			require.Greater(t, final.Seen["seen_original"], int64(0))
			require.LessOrEqual(t, final.Seen["seen_original"], int64((12 * time.Hour).Milliseconds()))
			if tc.Fault == "before" {
				require.Equal(t, int64(-2), final.Seen["seen_discovery"])
			} else {
				require.Greater(t, final.Seen["seen_discovery"], int64(0))
				require.LessOrEqual(t, final.Seen["seen_discovery"], int64((12 * time.Hour).Milliseconds()))
			}
			for _, name := range []string{"run", "start", "edge", "ready", "backlog", "backlog_start"} {
				require.Empty(t, final.Indexes[name], name)
			}
			require.Empty(t, requests)
			require.Empty(t, atReplay)
			require.Empty(t, resultsA)
			require.Empty(t, resultsB)
			count, err := effects.Do(ctx, effects.B().Get().Key("fixture:external-effect").Build()).AsInt64()
			require.NoError(t, err)
			require.EqualValues(t, 1, count, "missing or changed replay repeats the deliberately non-deduplicated effect")
		})
	}
}

// Admission control only: never lease discovery in A. Ordinary scanner reads,
// producer writes, original-job completion/retry, and B's admission are real.
// Waiting is bounded by the scanner context; no invented item/lease is returned.
type nativeReplayAdmission struct{ queue.QueueShard }

func (s *nativeReplayAdmission) Lease(ctx context.Context, item queue.QueueItem, duration time.Duration, now time.Time, opts ...queue.LeaseOptionFn) (*ulid.ULID, error) {
	if edge, ok := item.Data.Payload.(queue.PayloadEdge); ok && edge.Edge.Outgoing == "checkpoint-output" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.QueueShard.Lease(ctx, item, duration, now, opts...)
}

type nativeReplaySaveObservation struct {
	ID                statev2.ID
	Step, Output, Job string
	MetadataDelta     int
	Pending           bool
	Err, ContextErr   error
	After             nativeReplayReadout
}
type nativeReplaySave struct {
	statev2.RunService
	observations chan nativeReplaySaveObservation
	read         func(context.Context) nativeReplayReadout
}

func (s *nativeReplaySave) SaveStep(ctx context.Context, id statev2.ID, step string, data []byte) (bool, error) {
	pending, err := s.RunService.SaveStep(ctx, id, step, data)
	o := nativeReplaySaveObservation{ID: id, Step: step, Output: string(data), Job: queue.JobIDFromContext(ctx), MetadataDelta: state.MetadataSizeDeltaFromContext(ctx), Pending: pending, Err: err, ContextErr: ctx.Err()}
	o.After = s.read(ctx)
	s.observations <- o
	return pending, err
}

type nativeReplayEnqueueObservation struct {
	Before, After                   nativeReplayReadout
	LogicalJob, Group, ContextGroup string
	CommitErr, Returned, ContextErr error
}
type nativeReplayDiscovery struct {
	queue.Queue
	mode           string
	beforeErr      error
	observations   chan nativeReplayEnqueueObservation
	read           func(context.Context) nativeReplayReadout
	canceledClient rueidis.Client
	intercepted    atomic.Bool
}

func (q *nativeReplayDiscovery) Enqueue(ctx context.Context, item queue.Item, at time.Time, opts queue.EnqueueOpts) error {
	edge, ok := item.Payload.(queue.PayloadEdge)
	if !ok || item.Kind != queue.KindEdge || edge.Edge.Outgoing != "checkpoint-output" || !q.intercepted.CompareAndSwap(false, true) {
		return q.Queue.Enqueue(ctx, item, at, opts)
	}
	o := nativeReplayEnqueueObservation{Before: q.read(ctx), LogicalJob: *item.JobID, Group: item.GroupID, ContextGroup: state.GroupIDFromContext(ctx)}
	if q.mode == "before" {
		o.Returned = q.beforeErr
	} else {
		o.CommitErr = q.Queue.Enqueue(ctx, item, at, opts)
		o.Returned = o.CommitErr
		o.After = q.read(ctx)
		if q.mode == "after" && o.CommitErr == nil {
			// Suppress only this real successful enqueue's response. As in the
			// accepted adapter fault cases, get an actual error from a separate canceled
			// command, keeping the caller live for the real owner's AlwaysRetry path.
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			o.Returned = q.canceledClient.Do(canceled, q.canceledClient.B().Ping().Build()).Error()
		}
	}
	o.ContextErr = ctx.Err()
	q.observations <- o
	return o.Returned
}

// Literal read recipes are fixture-owned, rather than using the adapter's key
// generator or intercepted enqueue arguments as store truth. Raw item bytes and
// sorted-index scores are retained in every readout before finalization cleanup.
type nativeReplayReadout struct {
	Actions, Metadata, Items map[string]string
	Stack, Pending           []string
	Indexes                  map[string][]rueidis.ZScore
	Seen                     map[string]int64
	Err                      error
}

func readNativeReplayStore(ctx context.Context, v *queuehelper.ValkeyContainer, f nativeReplayFixture) (out nativeReplayReadout) {
	rc, err := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
	if err != nil {
		out.Err = err
		return
	}
	defer rc.Close()
	add := func(err error) { out.Err = errors.Join(out.Err, err) }
	out.Actions, err = rc.Do(ctx, rc.B().Hgetall().Key(f.StateKeys["actions"]).Build()).AsStrMap()
	add(err)
	out.Metadata, err = rc.Do(ctx, rc.B().Hgetall().Key(f.StateKeys["metadata"]).Build()).AsStrMap()
	add(err)
	out.Stack, err = rc.Do(ctx, rc.B().Lrange().Key(f.StateKeys["stack"]).Start(0).Stop(-1).Build()).AsStrSlice()
	add(err)
	out.Pending, err = rc.Do(ctx, rc.B().Smembers().Key(f.StateKeys["pending"]).Build()).AsStrSlice()
	add(err)
	out.Items, err = rc.Do(ctx, rc.B().Hgetall().Key(f.QueueKeys["items"]).Build()).AsStrMap()
	add(err)
	out.Indexes = map[string][]rueidis.ZScore{}
	for _, name := range []string{"run", "start", "edge", "ready", "backlog", "backlog_start"} {
		out.Indexes[name], err = rc.Do(ctx, rc.B().Zrange().Key(f.QueueKeys[name]).Min("0").Max("-1").Withscores().Build()).AsZScores()
		add(err)
	}
	out.Seen = map[string]int64{}
	for _, name := range []string{"seen_original", "seen_discovery"} {
		out.Seen[name], err = rc.Do(ctx, rc.B().Pttl().Key(f.QueueKeys[name]).Build()).AsInt64()
		add(err)
	}
	return
}

func requireNativeReplayCheckpoint(t *testing.T, f nativeReplayFixture, o nativeReplayReadout) {
	t.Helper()
	require.NoError(t, o.Err)
	require.Equal(t, map[string]string{"checkpoint-output": f.Output}, o.Actions)
	require.Equal(t, []string{"checkpoint-output"}, o.Stack)
	require.Empty(t, o.Pending)
	for key, value := range f.Metadata {
		if key == "id" {
			require.JSONEq(t, value, o.Metadata[key], key)
		} else {
			require.Equal(t, value, o.Metadata[key], key)
		}
	}
	require.Len(t, f.Output, f.OutputBytes)
}

func requireNativeReplayJobs(t *testing.T, f nativeReplayFixture, o nativeReplayReadout, expected []string, now, retryAt time.Time, keyQueues bool) {
	t.Helper()
	actual := []string{}
	for id, raw := range o.Items {
		actual = append(actual, id)
		var item queue.QueueItem
		require.NoError(t, json.Unmarshal([]byte(raw), &item))
		require.Equal(t, id, item.ID)
		require.Nil(t, item.LeaseID)
		require.Zero(t, item.Data.Attempt)
		require.Equal(t, "01K8F08000AAAAAAAAAAAAAAAA", item.Data.Identifier.RunID.String())
		require.Equal(t, "11111111-2222-3333-4444-555555555555", item.FunctionID.String())
		require.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", item.Data.Identifier.AccountID.String())
		require.Equal(t, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", item.WorkspaceID.String())
		require.Equal(t, "cccccccc-cccc-cccc-cccc-cccccccccccc", item.Data.Identifier.AppID.String())
		require.Empty(t, item.Data.CustomConcurrencyKeys)
		require.Empty(t, item.Data.Semaphores)
		require.Nil(t, item.Data.ParallelCoalesceKey)
		require.Nil(t, item.Data.Throttle)
		require.Nil(t, item.Data.Singleton)
		require.Nil(t, item.QueueName)
		status := "edge"
		if id == f.OriginalJob {
			require.Equal(t, queue.KindStart, item.Data.Kind)
			require.Equal(t, retryAt.UnixMilli(), item.AtMS)
			require.Equal(t, 2, item.GenerationID)
			require.Equal(t, 21, item.Data.GetMaxAttempts())
			status = "start"
		} else {
			require.Equal(t, f.DiscoveryJob, id)
			require.Equal(t, 1, item.GenerationID)
			require.Equal(t, queue.KindEdge, item.Data.Kind)
			require.Equal(t, queue.PayloadEdge{Edge: inngest.Edge{Outgoing: "checkpoint-output", Incoming: "step"}}, item.Data.Payload)
			require.Equal(t, 1, item.Data.GetMaxAttempts())
			require.GreaterOrEqual(t, item.AtMS, now.UnixMilli())
			require.Less(t, item.AtMS, now.Add(time.Minute).UnixMilli())
		}
		score := f.ImmediateScore
		if id == f.OriginalJob {
			score = retryAt.UnixMilli()
		}
		for _, index := range []string{"run", status} {
			require.True(t, slices.ContainsFunc(o.Indexes[index], func(z rueidis.ZScore) bool { return z.Member == id && z.Score == float64(score) }), index)
		}
		// Key queues can refill into ready before A is drained. Require real
		// runnable placement in either location, with no manufactured reschedule.
		placements := append(slices.Clone(o.Indexes["ready"]), o.Indexes["backlog"]...)
		placements = append(placements, o.Indexes["backlog_start"]...)
		require.True(t, slices.ContainsFunc(placements, func(z rueidis.ZScore) bool { return z.Member == id && z.Score == float64(score) }), "ready/backlog placement")
	}
	slices.Sort(actual)
	require.Equal(t, expected, actual)
	require.Len(t, o.Indexes["run"], len(expected))
	require.Len(t, o.Indexes["start"], boolNativeReplay(slices.Contains(expected, f.OriginalJob)))
	require.Len(t, o.Indexes["edge"], boolNativeReplay(slices.Contains(expected, f.DiscoveryJob)))
	require.Equal(t, len(expected), len(o.Indexes["ready"])+len(o.Indexes["backlog"])+len(o.Indexes["backlog_start"]))
	require.Equal(t, int64(-2), o.Seen["seen_discovery"], "live discovery has no completed-item tombstone")
	if slices.Contains(expected, f.OriginalJob) {
		require.Equal(t, int64(-2), o.Seen["seen_original"])
	} else {
		require.Greater(t, o.Seen["seen_original"], int64(0), "normal dequeue leaves the producer dedupe tombstone")
	}
	if !keyQueues {
		require.Empty(t, o.Indexes["backlog"])
		require.Empty(t, o.Indexes["backlog_start"])
	}
}

func requireNativeReplayCarrier(t *testing.T, raw, group string) {
	t.Helper()
	var item queue.QueueItem
	require.NoError(t, json.Unmarshal([]byte(raw), &item))
	require.Equal(t, group, item.Data.GroupID)
	require.Len(t, item.Data.Metadata, 1)
	var carrier map[string]string
	encoded, ok := item.Data.Metadata["user-otel-ctx"].(string)
	require.True(t, ok)
	require.NoError(t, json.Unmarshal([]byte(encoded), &carrier))
	require.ElementsMatch(t, []string{"tp", "ts", "dstp", "dsid"}, slices.Collect(maps.Keys(carrier)))
	require.Empty(t, carrier["ts"])
	ref := tracing.SpanRefFromQueueItem(&item.Data)
	require.NotNil(t, ref)
	require.Regexp(t, `^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$`, ref.TraceParent)
	require.Regexp(t, `^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$`, ref.DynamicSpanTraceParent)
	require.Equal(t, "167ba9dec9389d46f1eda58e0193efab", strings.Split(ref.TraceParent, "-")[1])
	require.Equal(t, "cce8a64da02e0531", strings.Split(ref.DynamicSpanTraceParent, "-")[2])
	require.Equal(t, strings.Split(ref.TraceParent, "-")[1], strings.Split(ref.DynamicSpanTraceParent, "-")[1])
	require.Equal(t, strings.Split(ref.TraceParent, "-")[2], ref.DynamicSpanID)
}

func requireNativeReplayHTTPRequest(t *testing.T, f nativeReplayFixture, r nativeReplayHTTPRequest, rawItem string) {
	t.Helper()
	var body struct {
		Steps   map[string]json.RawMessage `json:"steps"`
		Context struct {
			Job        string `json:"job_id"`
			QueueRef   string `json:"qi_id"`
			RequestID  string `json:"request_id"`
			Generation int    `json:"generation_id"`
		} `json:"ctx"`
	}
	require.NoError(t, json.Unmarshal(r.Body, &body))
	require.Equal(t, f.Output, string(body.Steps["checkpoint-output"]), "actual HTTP replay preserves literal saved wrapper bytes")
	var item queue.QueueItem
	require.NoError(t, json.Unmarshal([]byte(rawItem), &item))
	require.Equal(t, item.ID, body.Context.Job)
	require.Equal(t, item.GenerationID, body.Context.Generation)
	require.Equal(t, body.Context.Job, r.Header.Get("x-inngest-job-id"))
	require.Equal(t, fmt.Sprint(body.Context.Generation), r.Header.Get("x-inngest-generation-id"))
	require.Equal(t, body.Context.RequestID, r.Header.Get("x-request-id"))
	_, err := ulid.ParseStrict(body.Context.RequestID)
	require.NoError(t, err)
	require.Equal(t, "01K8F08000AAAAAAAAAAAAAAAA", r.Header.Get("X-Run-ID"))
	require.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", r.Header.Get("account-id"))
	decoded, err := base64.StdEncoding.DecodeString(body.Context.QueueRef)
	require.NoError(t, err)
	require.Equal(t, item.ID+"::"+consts.DefaultQueueShardName, string(decoded))
}

func takeNativeReplayObservation[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("missing native-step boundary observation")
		var zero T
		return zero
	}
}

func boolNativeReplay(b bool) int {
	if b {
		return 1
	}
	return 0
}

type nativeReplayHTTPRequest struct {
	recoveryHTTPRequest
	Header http.Header
}
