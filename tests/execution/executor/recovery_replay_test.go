package executor

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/config/registration"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	cqrsmanager "github.com/inngest/inngest/pkg/cqrs/manager"
	dbsqlite "github.com/inngest/inngest/pkg/db/sqlite"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/driver/httpdriver"
	"github.com/inngest/inngest/pkg/execution/exechttp"
	executorpkg "github.com/inngest/inngest/pkg/execution/executor"
	"github.com/inngest/inngest/pkg/execution/pauses"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
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

const recoveryFixtureDir = "testdata/sdk-response-replay-v1"

// This is a surviving-store witness, not a process-crash or exactly-once test.
// Only the SDK endpoint and stores survive: B receives no A-owned adapter,
// loader, executor, driver, queue, clock, or captured queue item.
func TestRecoveryReplaysPersistedSDKResponse(t *testing.T) {
	ctx := logger.WithStdlib(context.Background(), logger.VoidLogger())
	valkey, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	directory := t.TempDir()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	retryAt := now.Add(10 * time.Minute)
	id := statev2.ID{
		RunID:      ulid.MustParse("01K8F08000AAAAAAAAAAAAAAAA"),
		FunctionID: uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		Tenant: statev2.Tenant{
			AccountID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
			EnvID:     uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
			AppID:     uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc"),
		},
	}
	scope := queue.Scope{AccountID: id.Tenant.AccountID, EnvID: id.Tenant.EnvID, FunctionID: id.FunctionID}
	keyQueues := os.Getenv("EXPERIMENTAL_KEY_QUEUES_ENABLE") == "true"
	t.Logf("key queues: %t", keyQueues)

	// The endpoint knows only the HTTP protocol and its deliberately non-deduped
	// effect. In particular, it cannot query execution state or count callbacks
	// to decide which response to return.
	effects := recoveryValkeyClient(t, valkey)
	requests := make(chan recoveryHTTPRequest, 10)
	firstResponse := recoveryFixture(t, "checkpoint.http")
	replayResponse := recoveryFixture(t, "replay.http")
	sdk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		var request struct {
			Steps map[string]json.RawMessage `json:"steps"`
		}
		decodeErr := json.Unmarshal(body, &request)
		if readErr != nil || decodeErr != nil {
			http.Error(w, "invalid SDK request", http.StatusBadRequest)
			return
		}
		select {
		case requests <- recoveryHTTPRequest{Body: body, Version: r.Header.Get("x-inngest-req-version"), StepID: r.URL.Query().Get("stepId")}:
		default:
			http.Error(w, "unexpected SDK request flood", http.StatusTooManyRequests)
			return
		}
		response := replayResponse
		if _, saved := request.Steps["checkpoint-output"]; !saved {
			if err := effects.Do(r.Context(), effects.B().Incr().Key("fixture:external-effect").Build()).Error(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response = firstResponse
		}
		for name, values := range response.Header {
			w.Header()[name] = values
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(response.Body)
	}))
	t.Cleanup(sdk.Close)

	clockA := clockwork.NewFakeClockAt(now)
	a := newRecoveryRuntime(t, ctx, valkey, directory, clockA, retryAt, keyQueues)
	zero := 0
	fn := inngest.Function{ID: id.FunctionID, FunctionVersion: 1, Name: "recovery-fixture", Slug: "recovery-fixture",
		Steps: []inngest.Step{{ID: "step", Name: "step", URI: sdk.URL, Retries: &zero}}}
	config, err := json.Marshal(fn)
	require.NoError(t, err)
	_, err = a.cqrs.UpsertApp(ctx, cqrs.UpsertAppParams{ID: id.Tenant.AppID, Name: "recovery-fixture"})
	require.NoError(t, err)
	_, err = a.cqrs.UpsertFunction(ctx, cqrs.UpsertFunctionParams{ID: fn.ID, AppID: id.Tenant.AppID, Name: fn.Name, Slug: fn.Slug, Config: string(config)})
	require.NoError(t, err)

	enqueueErr := errors.New("fixture: discovery enqueue unavailable")
	failed := &recoveryFailDiscovery{Queue: a.queue, state: a.state, err: enqueueErr, boundary: make(chan recoveryCheckpoint, 1)}
	a.makeExecutor(t, ctx, failed)
	eventID := ulid.MustParse("01K8F08000BBBBBBBBBBBBBBBB")
	idempotency := "sdk-response-recovery-v1"
	_, scheduled, err := a.exec.Schedule(ctx, execution.ScheduleRequest{
		Function: fn, RunID: &id.RunID, At: &now, IdempotencyKey: &idempotency,
		AccountID: id.Tenant.AccountID, WorkspaceID: id.Tenant.EnvID, AppID: id.Tenant.AppID,
		Events: []event.TrackedEvent{event.NewBaseTrackedEventWithID(event.Event{
			ID: "source-event-v1", Name: "fixture/recovery", Timestamp: now.UnixMilli(), Data: map[string]any{"order": "order-1"},
		}, eventID)},
	})
	require.NoError(t, err)
	require.Equal(t, id, scheduled.ID)
	jobs, err := a.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	originalJobID := jobs[0].JobID
	require.NotEmpty(t, originalJobID)

	resultsA := a.start(t, ctx)
	recoveryEventually(t, clockA, func() bool { return len(resultsA) > 0 })
	resultA := <-resultsA
	require.Equal(t, originalJobID, resultA.JobID)
	require.ErrorContains(t, resultA.Err, enqueueErr.Error())
	require.True(t, queue.IsAlwaysRetryable(resultA.Err), "return the executor's real system retry marker to the queue")
	require.Equal(t, 0, resultA.Attempt)
	select {
	case boundary := <-failed.boundary:
		require.NoError(t, boundary.Err)
		recoveryAssertJSON(t, "checkpoint.state.json", boundary.JSON)
	default:
		t.Fatal("discovery enqueue boundary was not observed")
	}
	recoveryAssertRequest(t, "initial.request.json", recoveryTakeRequest(t, requests), originalJobID)

	// A callback signal precedes ProcessItem's Requeue. Wait for the actual
	// durable retry and released item lease before replacing any owner.
	recoveryEventually(t, clockA, func() bool {
		jobs, err := a.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
		if err != nil || len(jobs) != 1 || jobs[0].JobID != originalJobID {
			return false
		}
		item, ok := jobs[0].Raw.(*queue.QueueItem)
		return ok && item.LeaseID == nil && item.AtMS == retryAt.UnixMilli() && item.Data.Attempt == 0
	})
	a.close(t, ctx)
	a = nil
	failed = nil

	clockB := clockwork.NewFakeClockAt(now)
	b := newRecoveryRuntime(t, ctx, valkey, directory, clockB, retryAt, keyQueues)
	b.makeExecutor(t, ctx, b.queue)
	// No registration or enqueue here. The loader and scanner read the stores.
	loaded, err := b.loader.LoadFunction(ctx, id.Tenant.EnvID, id.FunctionID)
	require.NoError(t, err)
	require.Equal(t, sdk.URL, loaded.Function.Steps[0].URI)
	require.Equal(t, 1, loaded.Function.MaxAttempts())
	checkpoint := readRecoveryCheckpoint(ctx, b.state, id)
	require.NoError(t, checkpoint.Err)
	recoveryAssertJSON(t, "checkpoint.state.json", checkpoint.JSON)
	resultsB := b.start(t, ctx)
	clockB.Advance(retryAt.Sub(now))
	recoveryEventually(t, clockB, func() bool { return len(resultsB) > 0 })
	resultB := <-resultsB
	require.NoError(t, resultB.Err)
	require.Equal(t, originalJobID, resultB.JobID, "B must discover the persisted original job")
	require.Equal(t, 0, resultB.Attempt)
	recoveryAssertRequest(t, "replay.request.json", recoveryTakeRequest(t, requests), originalJobID)
	recoveryEventually(t, clockB, func() bool {
		jobs, err := b.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
		if err != nil || len(jobs) != 1 || jobs[0].JobID == originalJobID {
			return false
		}
		item, ok := jobs[0].Raw.(*queue.QueueItem)
		return ok && item.LeaseID == nil && item.Data.Kind == queue.KindSleep &&
			item.Data.Payload.(queue.PayloadEdge).Edge.Outgoing == "next-sleep" && item.AtMS > clockB.Now().UnixMilli()
	})
	b.close(t, ctx)
	b = nil

	// Independent readers after B has drained: no cached adapter or callback
	// values stand in for either the persisted checkpoint or remaining work.
	reader := newRecoveryRuntime(t, ctx, valkey, directory, clockwork.NewFakeClockAt(retryAt), retryAt, keyQueues)
	checkpoint = readRecoveryCheckpoint(ctx, reader.state, id)
	require.NoError(t, checkpoint.Err)
	recoveryAssertJSON(t, "checkpoint.state.json", checkpoint.JSON)
	jobs, err = reader.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	item := jobs[0].Raw.(*queue.QueueItem)
	require.NotEqual(t, originalJobID, jobs[0].JobID)
	require.Equal(t, queue.KindSleep, item.Data.Kind)
	require.Equal(t, "next-sleep", item.Data.Payload.(queue.PayloadEdge).Edge.Outgoing)
	require.GreaterOrEqual(t, item.AtMS, retryAt.Add(time.Hour).UnixMilli())
	require.LessOrEqual(t, item.AtMS, retryAt.Add(time.Hour+time.Minute).UnixMilli())
	reader.close(t, ctx)
	freshEffects := recoveryValkeyClient(t, valkey)
	count, err := freshEffects.Do(ctx, freshEffects.B().Get().Key("fixture:external-effect").Build()).AsInt64()
	require.NoError(t, err)
	require.EqualValues(t, 1, count, "missing replay would repeat the non-deduped external effect")
	require.Empty(t, requests)
	require.Empty(t, resultsA)
	require.Empty(t, resultsB)
}

type recoveryRuntime struct {
	db        *sql.DB
	clients   []rueidis.Client
	cqrs      cqrs.Manager
	loader    state.FunctionLoader
	state     statev2.RunService
	pauses    pauses.Manager
	registry  queue.ShardRegistryController
	queue     queue.Queue
	processor queue.QueueProcessor
	clock     clockwork.Clock
	exec      execution.Executor
	cancel    context.CancelFunc
	done      chan error
	closeOnce sync.Once
	closeHTTP func()
}

func newRecoveryRuntime(t *testing.T, ctx context.Context, valkey *queuehelper.ValkeyContainer, directory string, clock clockwork.Clock, retryAt time.Time, keyQueues bool) *recoveryRuntime {
	t.Helper()
	r := &recoveryRuntime{clock: clock}
	var err error
	r.db, err = dbsqlite.Open(ctx, dbsqlite.Options{Persist: true, ForTest: true, Directory: directory})
	require.NoError(t, err)
	t.Cleanup(func() { r.close(t, ctx) })
	r.cqrs = cqrsmanager.New(dbsqlite.New(r.db))
	r.loader = r.cqrs.(state.FunctionLoader)
	// Each owner gets fresh connections, including both Redis adapter roles.
	for range 2 {
		rc, err := queuehelper.NewValkeyClient(valkey.Addr, valkey.Username, valkey.Password, false)
		require.NoError(t, err)
		r.clients = append(r.clients, rc)
	}
	unsharded := redis_state.NewUnshardedClient(r.clients[0], redis_state.StateDefaultKey, redis_state.QueueDefaultKey)
	sharded := redis_state.NewShardedClient(redis_state.ShardedClientOpts{
		UnshardedClient: unsharded, FunctionRunStateClient: r.clients[1], StateDefaultKey: redis_state.StateDefaultKey,
		FnRunIsSharded: redis_state.AlwaysShardOnRun, BatchClient: r.clients[1], QueueDefaultKey: redis_state.QueueDefaultKey,
	})
	r.pauses = pauses.NewPauseStoreManager(unsharded)
	sm, err := redis_state.New(ctx, redis_state.WithShardedClient(sharded), redis_state.WithPauseDeleter(r.pauses))
	require.NoError(t, err)
	r.state = redis_state.MustRunServiceV2(sm)
	opts := []queue.QueueOpt{
		queue.WithClock(clock), queue.WithNumWorkers(10), queue.WithPollTick(10 * time.Millisecond),
		queue.WithShadowPollTick(10 * time.Millisecond),
		queue.WithRunMode(queue.QueueRunMode{Partition: true, ShadowPartition: keyQueues}),
		queue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool { return keyQueues }),
		queue.WithBackoffFunc(func(int) time.Time { return retryAt }),
	}
	shard := redis_state.NewQueueShard(consts.DefaultQueueShardName, unsharded.Queue(), opts...)
	r.registry, err = queue.NewSingleShardRegistry(shard)
	require.NoError(t, err)
	worker, err := queue.New(ctx, "recovery-witness", r.registry, opts...)
	require.NoError(t, err)
	r.queue, r.processor = worker, worker
	return r
}

func (r *recoveryRuntime) makeExecutor(t *testing.T, ctx context.Context, q queue.Queue) {
	t.Helper()
	// Config.NewDriver exercises MarshalV1, real HTTP, and ParseGenerator.
	httpClient := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), Timeout: 5 * time.Second}
	r.closeHTTP = httpClient.CloseIdleConnections
	driver, err := (httpdriver.Config{}).NewDriver(registration.NewDriverOpts{HTTPClient: exechttp.ExtendedClient{Client: httpClient}})
	require.NoError(t, err)
	r.exec, err = executorpkg.NewExecutor(
		executorpkg.WithStateManager(r.state), executorpkg.WithPauseManager(r.pauses), executorpkg.WithQueue(q),
		executorpkg.WithLogger(logger.StdlibLogger(ctx)), executorpkg.WithFunctionLoader(r.loader),
		executorpkg.WithShardRegistry(r.registry), executorpkg.WithClock(r.clock), executorpkg.WithDriverV1(driver),
		executorpkg.WithTracerProvider(tracing.NewSqlcTracerProvider(dbsqlite.New(r.db).Q())),
	)
	require.NoError(t, err)
}

type recoveryAttempt struct {
	JobID   string
	Attempt int
	Err     error
}

func (r *recoveryRuntime) start(t *testing.T, ctx context.Context) chan recoveryAttempt {
	t.Helper()
	ctx, r.cancel = context.WithCancel(ctx)
	r.done = make(chan error, 1)
	results := make(chan recoveryAttempt, 10)
	go func() {
		r.done <- r.processor.Run(ctx, func(ctx context.Context, _ queue.RunInfo, item queue.Item) (queue.RunResult, error) {
			_, err := r.exec.Execute(ctx, item.Identifier, item, item.Payload.(queue.PayloadEdge).Edge)
			results <- recoveryAttempt{queue.JobIDFromContext(ctx), item.Attempt, err}
			return queue.RunResult{}, err
		})
	}()
	return results
}

func (r *recoveryRuntime) close(t *testing.T, ctx context.Context) {
	t.Helper()
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
			select {
			case err := <-r.done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("queue shutdown: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("recovery queue did not drain")
			}
		}
		if r.exec != nil {
			r.exec.CloseLifecycleListeners(ctx)
		}
		if r.closeHTTP != nil {
			r.closeHTTP()
		}
		require.NoError(t, r.db.Close())
		for _, rc := range r.clients {
			rc.Close()
		}
	})
}

type recoveryFailDiscovery struct {
	queue.Queue
	state    statev2.StateLoader
	err      error
	failed   atomic.Bool
	boundary chan recoveryCheckpoint
}

func (q *recoveryFailDiscovery) Enqueue(ctx context.Context, item queue.Item, at time.Time, opts queue.EnqueueOpts) error {
	edge, ok := item.Payload.(queue.PayloadEdge)
	if ok && item.Kind == queue.KindEdge && edge.Edge.Outgoing == "checkpoint-output" && q.failed.CompareAndSwap(false, true) {
		q.boundary <- readRecoveryCheckpoint(ctx, q.state, statev2.IDFromV1(item.Identifier))
		return q.err // Fail before forwarding to the actual queue.
	}
	return q.Queue.Enqueue(ctx, item, at, opts)
}

type recoveryCheckpoint struct {
	JSON []byte
	Err  error
}

func readRecoveryCheckpoint(ctx context.Context, sl statev2.StateLoader, id statev2.ID) recoveryCheckpoint {
	md, err := sl.LoadMetadata(ctx, id)
	if err != nil {
		return recoveryCheckpoint{Err: err}
	}
	steps, err := sl.LoadSteps(ctx, id)
	if err != nil {
		return recoveryCheckpoint{Err: err}
	}
	pending, err := sl.LoadPending(ctx, id)
	if err != nil {
		return recoveryCheckpoint{Err: err}
	}
	if pending == nil {
		pending = []string{}
	}
	data, err := json.Marshal(map[string]any{"version": md.Config.RequestVersion, "event_ids": md.Config.EventIDs, "steps": steps, "stack": md.Stack, "pending": pending})
	return recoveryCheckpoint{data, err}
}

func recoveryEventually(t *testing.T, clock *clockwork.FakeClock, predicate func() bool) {
	t.Helper()
	require.Eventually(t, func() bool { clock.Advance(10 * time.Millisecond); return predicate() }, 10*time.Second, 10*time.Millisecond)
}

func recoveryValkeyClient(t *testing.T, valkey *queuehelper.ValkeyContainer) rueidis.Client {
	t.Helper()
	rc, err := queuehelper.NewValkeyClient(valkey.Addr, valkey.Username, valkey.Password, false)
	require.NoError(t, err)
	t.Cleanup(rc.Close)
	return rc
}

type recoveryHTTPResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func recoveryFixture(t *testing.T, name string) recoveryHTTPResponse {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(recoveryFixtureDir, name))
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), nil)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return recoveryHTTPResponse{response.StatusCode, response.Header, body}
}

type recoveryHTTPRequest struct {
	Body            []byte
	Version, StepID string
}

func recoveryTakeRequest(t *testing.T, requests <-chan recoveryHTTPRequest) recoveryHTTPRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("expected outbound SDK request")
		return recoveryHTTPRequest{}
	}
}

func recoveryAssertRequest(t *testing.T, fixture string, request recoveryHTTPRequest, originalJobID string) {
	t.Helper()
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(request.Body, &body))
	var ctx map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body["ctx"], &ctx))
	var jobID string
	require.NoError(t, json.Unmarshal(ctx["job_id"], &jobID))
	require.Equal(t, originalJobID, jobID, "HTTP request must identify the dispatched durable job")
	// Exclude dispatch IDs and queue references (random trace/group components).
	// Stable durable job identity is independently checked at both dispatches.
	projection := map[string]any{"request_version_header": request.Version, "step_id_query": request.StepID}
	for _, key := range []string{"event", "events", "steps", "version"} {
		projection[key] = body[key]
	}
	contextProjection := map[string]json.RawMessage{}
	for _, key := range []string{"fn_id", "run_id", "attempt", "max_attempts", "stack", "step_id"} {
		contextProjection[key] = ctx[key]
	}
	projection["ctx"] = contextProjection
	data, err := json.Marshal(projection)
	require.NoError(t, err)
	recoveryAssertJSON(t, fixture, data)
}

func recoveryAssertJSON(t *testing.T, fixture string, actual []byte) {
	t.Helper()
	expected, err := os.ReadFile(filepath.Join(recoveryFixtureDir, fixture))
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual), fixture)
}
